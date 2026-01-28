# FortCore - Advanced Rollback Manager
# Complete map restoration with fluid physics and block state tracking

from enum import Enum
from typing import Dict, List, Optional, Set, Tuple
from pathlib import Path
from datetime import datetime
import csv
from endstone import ColorFormat

class GameState(Enum):
    """Player game states"""
    LOBBY = "LOBBY"
    TELEPORTING = "TELEPORTING"
    MATCH = "MATCH"
    ROLLBACK = "ROLLBACK"

class RollbackAction:
    """Represents a single rollback action"""
    def __init__(self, action_type: str, x: int, y: int, z: int, block_type: str, timestamp: float):
        self.action_type = action_type
        self.x = x
        self.y = y
        self.z = z
        self.block_type = block_type
        self.timestamp = timestamp

class PlayerData:
    """Stores player state and rollback data"""
    def __init__(self, uuid: str):
        self.uuid = uuid
        self.state = GameState.LOBBY
        self.rollback_buffer: List[RollbackAction] = []
        self.csv_path: Optional[Path] = None
        self.last_flush = datetime.now().timestamp()
        self.current_category: Optional[str] = None
        self.current_match: Optional[str] = None
        self.pending_rollback_actions: List[Dict] = []
        self.rollback_enabled = True
        self.affected_blocks: Set[Tuple[int, int, int]] = set()  # Track all affected positions

class RollbackManager:
    """Advanced rollback system with fluid physics and block state tracking"""
    
    def __init__(self, plugin, rollback_dir: Path):
        self.plugin = plugin
        self.rollback_dir = rollback_dir
        self.rollback_tasks: Dict[str, int] = {}
        self.flush_task = None
        
        # Smart batching configuration
        self.SMALL_BATCH = 10     # For < 100 blocks: 10 per tick
        self.MEDIUM_BATCH = 25    # For 100-500 blocks: 25 per tick
        self.LARGE_BATCH = 40     # For 500-1000 blocks: 40 per tick
        self.HUGE_BATCH = 60      # For > 1000 blocks: 60 per tick
        
        # Fluid blocks that can spread
        self.FLUID_BLOCKS = {
            "minecraft:water", "minecraft:flowing_water",
            "minecraft:lava", "minecraft:flowing_lava"
        }
        
        # Blocks affected by fluids
        self.FLUID_AFFECTED = {
            "minecraft:grass_block": "minecraft:dirt",
            "minecraft:dirt_path": "minecraft:dirt",
            "minecraft:farmland": "minecraft:dirt",
            "minecraft:grass_path": "minecraft:dirt"
        }
        
        # Start flush task (every 20 seconds for better reliability)
        self.flush_task = self.plugin.server.scheduler.run_task(
            self.plugin, 
            self.flush_all_buffers, 
            delay=400, 
            period=400
        )
    
    def shutdown(self) -> None:
        """Shutdown rollback manager"""
        self.flush_all_buffers()
        if self.flush_task:
            try:
                self.plugin.server.scheduler.cancel_task(self.flush_task)
            except:
                pass
        for task_id in list(self.rollback_tasks.values()):
            try:
                self.plugin.server.scheduler.cancel_task(task_id)
            except:
                pass
    
    def resume_rollbacks(self) -> None:
        """Resume incomplete rollbacks from server restart"""
        try:
            if not self.rollback_dir.exists():
                return
            
            csv_files = list(self.rollback_dir.glob("rollback_*.csv"))
            if not csv_files:
                return
            
            self.plugin.logger.info(f"Found {len(csv_files)} incomplete rollback(s) from restart")
            
            for csv_file in csv_files:
                try:
                    uuid_str = csv_file.stem.replace("rollback_", "")
                    
                    with open(csv_file, 'r') as f:
                        lines = f.readlines()
                    
                    if len(lines) <= 1:
                        csv_file.unlink()
                        continue
                    
                    self.plugin.logger.info(f"Resuming rollback for {uuid_str}")
                    
                    data = self.plugin.get_player_data(uuid_str)
                    data.csv_path = csv_file
                    data.state = GameState.ROLLBACK
                    data.rollback_enabled = True
                    
                    actions = self.read_rollback_csv(csv_file)
                    if actions:
                        data.pending_rollback_actions = actions
                        batch_size, interval = self.calculate_batch_params(len(actions))
                        
                        task = self.plugin.server.scheduler.run_task(
                            self.plugin, 
                            lambda uid=uuid_str: self.process_rollback_batch(uid),
                            delay=interval,
                            period=interval
                        )
                        self.rollback_tasks[uuid_str] = task.task_id
                        self.plugin.logger.info(f"Resumed rollback: {len(actions)} actions")
                    else:
                        csv_file.unlink()
                        
                except Exception as e:
                    self.plugin.logger.error(f"Error resuming {csv_file.name}: {e}")
                    try:
                        csv_file.unlink()
                    except:
                        pass
                        
        except Exception as e:
            self.plugin.logger.error(f"Error during rollback resume: {e}")
    
    def calculate_batch_params(self, total_actions: int) -> tuple:
        """Calculate optimal batch size and interval based on action count"""
        if total_actions < 100:
            return self.SMALL_BATCH, 1
        elif total_actions < 500:
            return self.MEDIUM_BATCH, 1
        elif total_actions < 1000:
            return self.LARGE_BATCH, 1
        else:
            return self.HUGE_BATCH, 1
    
    def init_rollback(self, player_uuid: str, data: PlayerData) -> None:
        """Initialize rollback system"""
        data.rollback_buffer.clear()
        data.pending_rollback_actions.clear()
        data.affected_blocks.clear()
        data.rollback_enabled = True
        
        csv_path = self.rollback_dir / f"rollback_{player_uuid}.csv"
        data.csv_path = csv_path
        
        with open(csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "action", "x", "y", "z", "block_type"])
        
        data.last_flush = datetime.now().timestamp()
    
    def track_affected_area(self, data: PlayerData, x: int, y: int, z: int, block_type: str) -> None:
        """Track blocks affected by fluids and their surroundings"""
        pos = (x, y, z)
        data.affected_blocks.add(pos)
        
        # If placing/breaking fluid, track surrounding area for physics
        if block_type in self.FLUID_BLOCKS:
            for dx in [-1, 0, 1]:
                for dy in [-1, 0, 1]:
                    for dz in [-1, 0, 1]:
                        if dx == 0 and dy == 0 and dz == 0:
                            continue
                        affected_pos = (x + dx, y + dy, z + dz)
                        data.affected_blocks.add(affected_pos)
    
    def flush_all_buffers(self) -> None:
        """Flush all player buffers to disk"""
        for player_uuid, data in self.plugin.player_data.items():
            if data.state == GameState.MATCH and data.rollback_buffer and data.rollback_enabled:
                self.flush_buffer(data)
    
    def flush_buffer(self, data: PlayerData) -> None:
        """Flush single player buffer to CSV"""
        if not data.csv_path or not data.rollback_buffer:
            return
        
        try:
            count = len(data.rollback_buffer)
            with open(data.csv_path, 'a', newline='') as f:
                writer = csv.writer(f)
                for action in data.rollback_buffer:
                    writer.writerow([action.timestamp, action.action_type, action.x, action.y, action.z, action.block_type])
            
            self.plugin.logger.info(f"Flushed {count} actions for {data.uuid}")
            data.rollback_buffer.clear()
            data.last_flush = datetime.now().timestamp()
        except Exception as e:
            self.plugin.logger.error(f"Error flushing buffer: {e}")
    
    def start_rollback(self, player_uuid: str, data: PlayerData, player) -> None:
        """Start the rollback process - only if match is empty"""
        if data.state == GameState.ROLLBACK:
            self.plugin.logger.info(f"Player {player_uuid} already in rollback")
            return
        
        # Store match name before clearing
        match_name = data.current_match
        
        # Clear match/category IMMEDIATELY
        data.current_category = None
        data.current_match = None
        
        # Check if there are other players in the same match
        other_players = sum(1 for pd in self.plugin.player_data.values() 
                          if pd.uuid != player_uuid and pd.state == GameState.MATCH and pd.current_match == match_name)
        
        if other_players > 0:
            # Match is not empty, skip rollback
            self.plugin.logger.info(f"Skipping rollback - {other_players} player(s) still in {match_name}")
            
            # Clean up CSV file
            if data.csv_path and data.csv_path.exists():
                try:
                    data.csv_path.unlink()
                except:
                    pass
            
            # Clear data
            data.rollback_buffer.clear()
            data.pending_rollback_actions.clear()
            data.affected_blocks.clear()
            data.csv_path = None
            
            if player:
                self.plugin.reset_player(player)
            else:
                data.state = GameState.LOBBY
            
            return
        
        # Match is empty, proceed with rollback
        self.plugin.logger.info(f"Starting complete map restoration for {match_name}")
        data.state = GameState.ROLLBACK
        
        # Force flush any remaining buffer
        self.flush_buffer(data)
        
        if data.csv_path and data.csv_path.exists():
            actions = self.read_rollback_csv(data.csv_path)
            action_count = len(actions)
            
            # Add affected area cleanup to actions
            affected_cleanup = self.generate_affected_area_cleanup(data)
            actions.extend(affected_cleanup)
            
            total_actions = len(actions)
            
            if actions:
                data.pending_rollback_actions = actions
                batch_size, interval = self.calculate_batch_params(total_actions)
                
                if player:
                    if total_actions > 500:
                        player.send_message(f"{ColorFormat.YELLOW}Restoring map... ({total_actions} blocks){ColorFormat.RESET}")
                
                self.plugin.logger.info(f"Starting rollback: {action_count} direct + {len(affected_cleanup)} affected = {total_actions} total")
                
                task = self.plugin.server.scheduler.run_task(
                    self.plugin, 
                    lambda: self.process_rollback_batch(player_uuid), 
                    delay=interval, 
                    period=interval
                )
                self.rollback_tasks[player_uuid] = task.task_id
            else:
                self.finish_rollback(player_uuid, player)
        else:
            self.finish_rollback(player_uuid, player)
    
    def generate_affected_area_cleanup(self, data: PlayerData) -> List[Dict]:
        """Generate cleanup actions for affected areas (fluid physics, block states)"""
        cleanup_actions = []
        
        # Create actions to reset affected blocks (set to air to trigger physics reset)
        for x, y, z in data.affected_blocks:
            cleanup_actions.append({
                "action": "cleanup",
                "x": str(x),
                "y": str(y),
                "z": str(z),
                "block_type": "minecraft:air"
            })
        
        return cleanup_actions
    
    def read_rollback_csv(self, csv_path: Path) -> List[Dict]:
        """Read rollback actions from CSV in reverse"""
        actions = []
        try:
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                actions = list(reader)
                actions.reverse()
        except Exception as e:
            self.plugin.logger.error(f"Error reading CSV: {e}")
        return actions
    
    def process_rollback_batch(self, player_uuid: str) -> None:
        """Process rollback batch with smart batching"""
        data = self.plugin.get_player_data(player_uuid)
        
        if data.state != GameState.ROLLBACK:
            self.plugin.logger.warning(f"Player {player_uuid} not in rollback state, stopping")
            if player_uuid in self.rollback_tasks:
                try:
                    self.plugin.server.scheduler.cancel_task(self.rollback_tasks[player_uuid])
                    del self.rollback_tasks[player_uuid]
                except:
                    pass
            return
        
        actions = data.pending_rollback_actions
        
        if not actions:
            self.plugin.logger.info(f"Rollback complete for {player_uuid}")
            try:
                player = self.plugin.server.get_player_by_name(data.uuid)
            except:
                player = None
            self.finish_rollback(player_uuid, player)
            return
        
        # Calculate batch size based on remaining actions
        batch_size, _ = self.calculate_batch_params(len(actions))
        
        # Process batch
        processed = 0
        for _ in range(min(batch_size, len(actions))):
            if not actions:
                break
            action = actions.pop(0)
            self.revert_action(action)
            processed += 1
        
        # Check if done
        if not actions:
            self.plugin.logger.info(f"All actions processed for {player_uuid}")
            try:
                player = self.plugin.server.get_player_by_name(data.uuid)
            except:
                player = None
            self.finish_rollback(player_uuid, player)
    
    def revert_action(self, action: Dict) -> None:
        """Revert a single action using vanilla setblock command"""
        try:
            x = int(action["x"])
            y = int(action["y"])
            z = int(action["z"])
            block_type = action["block_type"]
            action_type = action.get("action", "unknown")
            
            if action_type == "place":
                # Player placed this block, remove it
                self.plugin.server.dispatch_command(
                    self.plugin.server.command_sender,
                    f'setblock {x} {y} {z} air'
                )
            elif action_type == "break":
                # Player broke this block, restore it
                self.plugin.server.dispatch_command(
                    self.plugin.server.command_sender,
                    f'setblock {x} {y} {z} {block_type}'
                )
            elif action_type == "cleanup":
                # Cleanup affected area (force block update)
                self.plugin.server.dispatch_command(
                    self.plugin.server.command_sender,
                    f'setblock {x} {y} {z} air'
                )
                
        except Exception as e:
            self.plugin.logger.error(f"Error reverting action at ({x}, {y}, {z}): {e}")
    
    def finish_rollback(self, player_uuid: str, player) -> None:
        """Finish rollback and reset player"""
        data = self.plugin.get_player_data(player_uuid)
        
        if data.state != GameState.ROLLBACK:
            return
        
        self.plugin.logger.info(f"Finishing rollback for {player_uuid}")
        
        if player_uuid in self.rollback_tasks:
            try:
                task_id = self.rollback_tasks[player_uuid]
                self.plugin.server.scheduler.cancel_task(task_id)
                del self.rollback_tasks[player_uuid]
            except Exception as e:
                self.plugin.logger.error(f"Error canceling task: {e}")
        
        if data.csv_path and data.csv_path.exists():
            try:
                data.csv_path.unlink()
                self.plugin.logger.info(f"Deleted rollback CSV for {player_uuid}")
            except Exception as e:
                self.plugin.logger.error(f"Error deleting CSV: {e}")
        
        data.rollback_buffer.clear()
        data.pending_rollback_actions.clear()
        data.affected_blocks.clear()
        data.csv_path = None
        data.current_category = None
        data.current_match = None
        
        # Only send message and reset if player is online
        if player:
            self.plugin.logger.info(f"Resetting online player {player_uuid}")
            self.plugin.reset_player(player)
            player.send_message(f"{ColorFormat.GREEN}Map restored!{ColorFormat.RESET}")
        else:
            self.plugin.logger.info(f"Player {player_uuid} offline, setting to LOBBY state")
            data.state = GameState.LOBBY