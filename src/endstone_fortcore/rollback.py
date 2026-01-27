# FortCore - Rollback Manager
# Handles all rollback operations using vanilla setblock commands

from enum import Enum
from typing import Dict, List, Optional
from pathlib import Path
from datetime import datetime
import csv
from endstone import ColorFormat

class GameState(Enum):
    """Player game states"""
    LOBBY = "LOBBY"
    QUEUE = "QUEUE"
    TELEPORTING = "TELEPORTING"
    MATCH = "MATCH"
    ROLLBACK = "ROLLBACK"
    END = "END"

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
        self.world_name: Optional[str] = None

class RollbackManager:
    """Manages rollback operations using vanilla setblock commands"""
    
    def __init__(self, plugin, rollback_dir: Path):
        self.plugin = plugin
        self.rollback_dir = rollback_dir
        self.rollback_tasks: Dict[str, int] = {}
        self.flush_task = None
        
        # Start flush task
        self.flush_task = self.plugin.server.scheduler.run_task(
            self.plugin, 
            self.flush_all_buffers, 
            delay=1200, 
            period=1200
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
    
    def init_rollback(self, player_uuid: str, data: PlayerData) -> None:
        """Initialize rollback system"""
        data.rollback_buffer.clear()
        data.pending_rollback_actions.clear()
        
        csv_path = self.rollback_dir / f"rollback_{player_uuid}.csv"
        data.csv_path = csv_path
        
        with open(csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "action", "x", "y", "z", "block_type"])
        
        data.last_flush = datetime.now().timestamp()
    
    def flush_all_buffers(self) -> None:
        """Flush all player buffers to disk"""
        for player_uuid, data in self.plugin.player_data.items():
            if data.state == GameState.MATCH and data.rollback_buffer:
                self.flush_buffer(data)
    
    def flush_buffer(self, data: PlayerData) -> None:
        """Flush single player buffer to CSV"""
        if not data.csv_path or not data.rollback_buffer:
            return
        
        try:
            actions_count = len(data.rollback_buffer)
            with open(data.csv_path, 'a', newline='') as f:
                writer = csv.writer(f)
                for action in data.rollback_buffer:
                    writer.writerow([action.timestamp, action.action_type, action.x, action.y, action.z, action.block_type])
            
            self.plugin.logger.info(f"Flushed {actions_count} actions to disk for {data.uuid}")
            data.rollback_buffer.clear()
            data.last_flush = datetime.now().timestamp()
        except Exception as e:
            self.plugin.logger.error(f"Error flushing buffer: {e}")
    
    def start_rollback(self, player_uuid: str, data: PlayerData, player) -> None:
        """Start the rollback process - only if match is empty"""
        if data.state == GameState.ROLLBACK:
            self.plugin.logger.info(f"Player {player_uuid} already in rollback state")
            return
        
        # Store match name before clearing
        match_name = data.current_match
        
        # Clear match/category IMMEDIATELY to prevent count issues
        data.current_category = None
        data.current_match = None
        
        # Check if there are other players in the same match
        other_players = 0
        for pd in self.plugin.player_data.values():
            if pd.uuid != player_uuid and pd.state == GameState.MATCH and pd.current_match == match_name:
                other_players += 1
        
        if other_players > 0:
            # Match is not empty, skip rollback
            self.plugin.logger.info(f"Skipping rollback for {player_uuid} - {other_players} other player(s) still in match {match_name}")
            
            # Clean up CSV file
            if data.csv_path and data.csv_path.exists():
                try:
                    data.csv_path.unlink()
                    self.plugin.logger.info(f"Deleted rollback file for {player_uuid} (match not empty)")
                except Exception as e:
                    self.plugin.logger.error(f"Error deleting CSV: {e}")
            
            # Clear data and reset if player is online
            data.rollback_buffer.clear()
            data.pending_rollback_actions.clear()
            data.csv_path = None
            
            if player:
                self.plugin.reset_player(player)
                player.send_message(f"{ColorFormat.GREEN}Returned to lobby{ColorFormat.RESET}")
            else:
                data.state = GameState.LOBBY
            
            return
        
        # Match is empty, proceed with rollback
        self.plugin.logger.info(f"Starting rollback for {player_uuid} - match {match_name} is now empty")
        data.state = GameState.ROLLBACK
        self.flush_buffer(data)
        
        # Warn about potential lag
        if player:
            player.send_message(f"{ColorFormat.GOLD}[WARNING]{ColorFormat.RESET} {ColorFormat.YELLOW}Rolling back match {match_name}...{ColorFormat.RESET}")
            player.send_message(f"{ColorFormat.YELLOW}Expect lag during rollback!{ColorFormat.RESET}")
        
        if data.csv_path and data.csv_path.exists():
            actions = self.read_rollback_csv(data.csv_path)
            self.plugin.logger.info(f"Found {len(actions)} actions to rollback for {player_uuid}")
            if actions:
                data.pending_rollback_actions = actions
                task = self.plugin.server.scheduler.run_task(
                    self.plugin, 
                    lambda: self.process_rollback_batch(player_uuid), 
                    delay=10, 
                    period=10
                )
                self.rollback_tasks[player_uuid] = task.task_id
                self.plugin.logger.info(f"Started rollback task {task.task_id} for {player_uuid}")
            else:
                self.plugin.logger.info(f"No actions to rollback for {player_uuid}")
                self.finish_rollback(player_uuid, player)
        else:
            self.plugin.logger.info(f"No CSV file found for {player_uuid}")
            self.finish_rollback(player_uuid, player)
    
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
        """Process 2 rollback actions every 0.5 seconds"""
        data = self.plugin.get_player_data(player_uuid)
        actions = data.pending_rollback_actions
        
        if not actions:
            self.plugin.logger.info(f"Rollback complete for {player_uuid} - no more actions")
            try:
                player = self.plugin.server.get_player_by_name(data.uuid)
            except:
                player = None
            self.finish_rollback(player_uuid, player)
            return
        
        processed = 0
        for _ in range(min(2, len(actions))):
            if not actions:
                break
            action = actions.pop(0)
            self.revert_action(action)
            processed += 1
        
        self.plugin.logger.info(f"Processed {processed} rollback actions for {player_uuid}, {len(actions)} remaining")
        
        if not actions:
            self.plugin.logger.info(f"Rollback finished for {player_uuid}")
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
            action_type = action["action"]
            
            if action_type == "place":
                # Player placed this block, so remove it (set to air)
                self.plugin.logger.info(f"Reverting PLACE: Removing {block_type} at ({x}, {y}, {z})")
                self.plugin.server.dispatch_command(
                    self.plugin.server.command_sender,
                    f'setblock {x} {y} {z} air'
                )
            elif action_type == "break":
                # Player broke this block, so restore it
                self.plugin.logger.info(f"Reverting BREAK: Restoring {block_type} at ({x}, {y}, {z})")
                self.plugin.server.dispatch_command(
                    self.plugin.server.command_sender,
                    f'setblock {x} {y} {z} {block_type}'
                )
                
        except Exception as e:
            self.plugin.logger.error(f"Error reverting action at ({x}, {y}, {z}): {e}")
    
    def finish_rollback(self, player_uuid: str, player) -> None:
        """Finish rollback and reset player"""
        data = self.plugin.get_player_data(player_uuid)
        
        if data.state != GameState.ROLLBACK:
            return
        
        if player_uuid in self.rollback_tasks:
            try:
                task_id = self.rollback_tasks[player_uuid]
                self.plugin.server.scheduler.cancel_task(task_id)
            except Exception as e:
                self.plugin.logger.error(f"Error canceling task: {e}")
            finally:
                del self.rollback_tasks[player_uuid]
        
        if data.csv_path and data.csv_path.exists():
            try:
                data.csv_path.unlink()
            except Exception as e:
                self.plugin.logger.error(f"Error deleting CSV: {e}")
        
        data.rollback_buffer.clear()
        data.pending_rollback_actions.clear()
        data.csv_path = None
        
        # Already cleared in start_rollback, but clear again just in case
        data.current_category = None
        data.current_match = None
        
        if player:
            self.plugin.reset_player(player)
            player.send_message(f"{ColorFormat.GREEN}Rollback complete! You're back in the lobby.{ColorFormat.RESET}")
        else:
            data.state = GameState.LOBBY
