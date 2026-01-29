# FortCore - Simple Rollback Manager
# Basic block tracking and restoration

from enum import Enum
from typing import Dict, List, Optional
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

class RollbackManager:
    """Simple rollback system - just track breaks and placements"""
    
    def __init__(self, plugin, rollback_dir: Path):
        self.plugin = plugin
        self.rollback_dir = rollback_dir
        self.rollback_tasks: Dict[str, int] = {}
        self.flush_task = None
        
        # Smart batching configuration
        self.SMALL_BATCH = 10     # For < 100 blocks
        self.MEDIUM_BATCH = 25    # For 100-500 blocks
        self.LARGE_BATCH = 40     # For 500-1000 blocks
        self.HUGE_BATCH = 60      # For > 1000 blocks
        
        # Start flush task (every 20 seconds)
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
            
            self.plugin.logger.info(f"Found {len(csv_files)} incomplete rollback(s)")
            
            for csv_file in csv_files:
                try:
                    uuid_str = csv_file.stem.replace("rollback_", "")
                    
                    with open(csv_file, 'r') as f:
                        lines = f.readlines()
                    
                    if len(lines) <= 1:
                        csv_file.unlink()
                        continue
                    
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
                    else:
                        csv_file.unlink()
                        
                except Exception as e:
                    self.plugin.logger.error(f"Error resuming rollback: {e}")
                    try:
                        csv_file.unlink()
                    except:
                        pass
                        
        except Exception as e:
            self.plugin.logger.error(f"Error during rollback resume: {e}")
    
    def calculate_batch_params(self, total_actions: int) -> tuple:
        """Calculate optimal batch size and interval"""
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
        data.rollback_enabled = True
        
        csv_path = self.rollback_dir / f"rollback_{player_uuid}.csv"
        data.csv_path = csv_path
        
        with open(csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "action", "x", "y", "z", "block_type"])
        
        data.last_flush = datetime.now().timestamp()
    
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
            
            data.rollback_buffer.clear()
            data.last_flush = datetime.now().timestamp()
        except Exception as e:
            self.plugin.logger.error(f"Error flushing buffer: {e}")
    
    def start_rollback(self, player_uuid: str, data: PlayerData, player) -> None:
        """Start the rollback process - only if match is empty"""
        if data.state == GameState.ROLLBACK:
            return
        
        # Store match info before clearing
        match_name = data.current_match
        category = data.current_category
        
        # Clear match/category IMMEDIATELY
        data.current_category = None
        data.current_match = None
        
        # Check if there are other players in the same match (use both category and match)
        other_players = sum(1 for pd in self.plugin.player_data.values() 
                          if pd.uuid != player_uuid and 
                             pd.state == GameState.MATCH and 
                             pd.current_category == category and 
                             pd.current_match == match_name)
        
        if other_players > 0:
            # Match is not empty, skip rollback
            self.plugin.logger.info(f"Skipping rollback - {other_players} player(s) still in {category}:{match_name}")
            
            if data.csv_path and data.csv_path.exists():
                try:
                    data.csv_path.unlink()
                except:
                    pass
            
            data.rollback_buffer.clear()
            data.pending_rollback_actions.clear()
            data.csv_path = None
            
            if player:
                self.plugin.reset_player(player)
            else:
                data.state = GameState.LOBBY
            
            return
        
        # Match is empty, proceed with rollback
        self.plugin.logger.info(f"Starting rollback for {category}:{match_name}")
        data.state = GameState.ROLLBACK
        
        # Force flush any remaining buffer
        self.flush_buffer(data)
        
        if data.csv_path and data.csv_path.exists():
            actions = self.read_rollback_csv(data.csv_path)
            total_actions = len(actions)
            
            if actions:
                data.pending_rollback_actions = actions
                batch_size, interval = self.calculate_batch_params(total_actions)
                
                if player and total_actions > 500:
                    player.send_message(f"{ColorFormat.YELLOW}Rolling back {total_actions} blocks...{ColorFormat.RESET}")
                
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
    
    def read_rollback_csv(self, csv_path: Path) -> List[Dict]:
        """Read rollback actions from CSV in reverse order"""
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
        """Process rollback batch"""
        data = self.plugin.get_player_data(player_uuid)
        
        if data.state != GameState.ROLLBACK:
            if player_uuid in self.rollback_tasks:
                try:
                    self.plugin.server.scheduler.cancel_task(self.rollback_tasks[player_uuid])
                    del self.rollback_tasks[player_uuid]
                except:
                    pass
            return
        
        actions = data.pending_rollback_actions
        
        if not actions:
            try:
                player = self.plugin.server.get_player_by_name(data.uuid)
            except:
                player = None
            self.finish_rollback(player_uuid, player)
            return
        
        # Calculate batch size
        batch_size, _ = self.calculate_batch_params(len(actions))
        
        # Process batch
        for _ in range(min(batch_size, len(actions))):
            if not actions:
                break
            action = actions.pop(0)
            self.revert_action(action)
        
        # Check if done
        if not actions:
            try:
                player = self.plugin.server.get_player_by_name(data.uuid)
            except:
                player = None
            self.finish_rollback(player_uuid, player)
    
    def revert_action(self, action: Dict) -> None:
        """Revert a single action - handles liquids properly"""
        try:
            x = int(action["x"])
            y = int(action["y"])
            z = int(action["z"])
            block_type = action["block_type"]
            action_type = action.get("action", "unknown")
            
            # Check if block is a liquid
            is_liquid = block_type in ["minecraft:water", "minecraft:lava", "minecraft:flowing_water", "minecraft:flowing_lava"]
            
            if action_type == "place":
                # Player placed this block - remove it
                self.plugin.server.dispatch_command(
                    self.plugin.server.command_sender,
                    f'setblock {x} {y} {z} air'
                )
            elif action_type == "break":
                # Player broke this block - restore it (non-liquids only)
                if not is_liquid:
                    self.plugin.server.dispatch_command(
                        self.plugin.server.command_sender,
                        f'setblock {x} {y} {z} {block_type}'
                    )
                
        except Exception as e:
            self.plugin.logger.error(f"Error reverting: {e}")
    
    def finish_rollback(self, player_uuid: str, player) -> None:
        """Finish rollback and reset player"""
        data = self.plugin.get_player_data(player_uuid)
        
        if data.state != GameState.ROLLBACK:
            return
        
        if player_uuid in self.rollback_tasks:
            try:
                task_id = self.rollback_tasks[player_uuid]
                self.plugin.server.scheduler.cancel_task(task_id)
                del self.rollback_tasks[player_uuid]
            except:
                pass
        
        if data.csv_path and data.csv_path.exists():
            try:
                data.csv_path.unlink()
            except:
                pass
        
        data.rollback_buffer.clear()
        data.pending_rollback_actions.clear()
        data.csv_path = None
        data.current_category = None
        data.current_match = None
        
        if player:
            self.plugin.reset_player(player)
            player.send_message(f"{ColorFormat.GREEN}Rollback complete!{ColorFormat.RESET}")
        else:
            data.state = GameState.LOBBY