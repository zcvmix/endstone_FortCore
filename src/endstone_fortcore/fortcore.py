"""
FortCore - High-performance PvP Core Plugin for Endstone Bedrock Server
Optimized for zero lag with async operations and efficient rollback system
"""

from endstone.plugin import Plugin
from endstone.command import Command, CommandSender
from endstone.event import event_handler, PlayerJoinEvent, PlayerDeathEvent, PlayerQuitEvent, BlockBreakEvent, BlockPlaceEvent
from endstone import ColorFormat, GameMode
from endstone.form import ActionForm, Button
import yaml
import csv
import os
from pathlib import Path
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional
import uuid as uuid_lib

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
        self.action_type = action_type  # "break" or "place"
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
        self.current_map: Optional[str] = None
        self.current_kit: Optional[str] = None

class FortCore(Plugin):
    api_version = "0.5"
    
    commands = {
        "out": {
            "description": "Leave the current match and return to lobby",
            "usages": ["/out"],
            "permissions": ["fortcore.command.out"],
        }
    }
    
    permissions = {
        "fortcore.command.out": {
            "description": "Allow players to leave matches",
            "default": True,
        }
    }
    
    def __init__(self):
        super().__init__()
        self.player_data: Dict[str, PlayerData] = {}
        self.config: Dict = {}
        self.teleport_cooldown: Dict[str, float] = {}
        self.rollback_dir: Path = None
        self.flush_task = None
        self.rollback_tasks: Dict[str, int] = {}
        
    def on_load(self) -> None:
        self.logger.info("FortCore loading...")
        self.load_config()
        
    def on_enable(self) -> None:
        self.logger.info("FortCore enabled!")
        self.register_events(self)
        
        # Create rollback directory
        self.rollback_dir = Path(self.data_folder) / "rollbacks"
        self.rollback_dir.mkdir(parents=True, exist_ok=True)
        
        # Start flush task (every 60 seconds)
        self.flush_task = self.server.scheduler.run_task_timer(
            self, self.flush_all_buffers, delay=1200, period=1200
        )
        
        self.logger.info(f"Loaded {len(self.config.get('maps', []))} maps and {len(self.config.get('kits', []))} kits")
        
    def on_disable(self) -> None:
        self.logger.info("FortCore disabling...")
        # Flush all buffers before shutdown
        self.flush_all_buffers()
        # Cancel all tasks
        if self.flush_task:
            self.server.scheduler.cancel_task(self.flush_task)
        for task_id in self.rollback_tasks.values():
            self.server.scheduler.cancel_task(task_id)
        
    def load_config(self) -> None:
        """Load configuration from config.yml"""
        config_path = Path(self.data_folder) / "config.yml"
        
        if not config_path.exists():
            # Create default config
            default_config = {
                "lobby_spawn": {
                    "x": 0,
                    "y": 100,
                    "z": 0,
                    "world": "world"
                },
                "maps": [
                    {
                        "name": "Diamond Arena",
                        "creator": "Admin",
                        "spawn": {"x": 100, "y": 64, "z": 100},
                        "world": "world"
                    }
                ],
                "kits": [
                    {
                        "name": "Diamond SMP",
                        "creator": "Admin",
                        "maxPlayers": 8
                    }
                ]
            }
            
            config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(config_path, 'w') as f:
                yaml.dump(default_config, f, default_flow_style=False)
            self.config = default_config
        else:
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
                
    def get_player_data(self, player_uuid: str) -> PlayerData:
        """Get or create player data"""
        if player_uuid not in self.player_data:
            self.player_data[player_uuid] = PlayerData(player_uuid)
        return self.player_data[player_uuid]
    
    def reset_player(self, player) -> None:
        """Complete player reset - used everywhere"""
        try:
            # Set gamemode
            player.game_mode = GameMode.SURVIVAL
            
            # Clear all effects
            for effect in player.active_effects:
                player.remove_effect(effect.type)
            
            # Clear inventory (main, armor, offhand)
            inventory = player.inventory
            inventory.clear()
            
            # Teleport to lobby
            lobby = self.config.get("lobby_spawn", {})
            level = self.server.get_level(lobby.get("world", "world"))
            if level:
                player.teleport(level, lobby.get("x", 0), lobby.get("y", 100), lobby.get("z", 0))
            
            # Give menu item (lodestone compass) in slot 9 (index 8)
            from endstone.inventory import ItemStack
            menu_item = ItemStack("minecraft:lodestone_compass", 1)
            inventory.set_item(8, menu_item)
            
            # Apply weakness effect (level 255, infinite, no particles)
            from endstone.potion import PotionEffect, PotionEffectType
            weakness = PotionEffect(PotionEffectType.WEAKNESS, -1, 255, False, False, False)
            player.add_effect(weakness)
            
        except Exception as e:
            self.logger.error(f"Error resetting player: {e}")
    
    @event_handler
    def on_player_join(self, event: PlayerJoinEvent) -> None:
        """Handle player join"""
        player = event.player
        player_uuid = str(player.unique_id)
        
        # Schedule reset slightly delayed to ensure player is fully loaded
        self.server.scheduler.run_task(
            self, lambda: self.handle_join_sequence(player), delay=10
        )
    
    def handle_join_sequence(self, player) -> None:
        """Handle join sequence"""
        player_uuid = str(player.unique_id)
        data = self.get_player_data(player_uuid)
        
        # Reset player completely
        self.reset_player(player)
        
        # Set state to LOBBY
        data.state = GameState.LOBBY
        
        # Welcome message
        player.send_message(f"{ColorFormat.GOLD}=== FortCore ==={ColorFormat.RESET}")
        player.send_message(f"{ColorFormat.YELLOW}Right-click the compass to join a match!{ColorFormat.RESET}")
    
    def open_kit_menu(self, player) -> None:
        """Open the kit selection menu"""
        form = ActionForm()
        form.title = "FortCore"
        
        kits = self.config.get("kits", [])
        maps = self.config.get("maps", [])
        
        # Create buttons for each kit
        for i, kit in enumerate(kits):
            # Count online players in this kit
            online_count = sum(1 for pd in self.player_data.values() 
                             if pd.state == GameState.MATCH and pd.current_kit == kit.get("name"))
            max_players = kit.get("maxPlayers", 8)
            
            button_text = f"{kit.get('name', 'Unknown')} [{online_count}/{max_players}]"
            form.button(button_text, on_click=lambda p, idx=i: self.handle_kit_select(p, idx))
        
        form.send(player)
    
    def handle_kit_select(self, player, kit_index: int) -> None:
        """Handle kit selection"""
        player_uuid = str(player.unique_id)
        data = self.get_player_data(player_uuid)
        
        kits = self.config.get("kits", [])
        maps = self.config.get("maps", [])
        
        if kit_index >= len(kits) or kit_index >= len(maps):
            player.send_message(f"{ColorFormat.RED}Invalid selection!{ColorFormat.RESET}")
            return
        
        kit = kits[kit_index]
        map_data = maps[kit_index]
        
        # Check if player is already in match
        if data.state == GameState.MATCH or data.state == GameState.TELEPORTING:
            player.send_message(f"{ColorFormat.RED}You are already in a match!{ColorFormat.RESET}")
            return
        
        # Check if kit is full
        online_count = sum(1 for pd in self.player_data.values() 
                         if pd.state == GameState.MATCH and pd.current_kit == kit.get("name"))
        if online_count >= kit.get("maxPlayers", 8):
            player.send_message(f"{ColorFormat.RED}This match is full!{ColorFormat.RESET}")
            return
        
        # Check global cooldown
        current_time = datetime.now().timestamp()
        last_teleport = self.teleport_cooldown.get(kit.get("name"), 0)
        if current_time - last_teleport < 5.0:
            player.send_message(f"{ColorFormat.RED}Someone just teleported! Wait a moment...{ColorFormat.RESET}")
            return
        
        # Set teleporting state
        data.state = GameState.TELEPORTING
        self.teleport_cooldown[kit.get("name")] = current_time
        
        # Schedule teleport
        self.server.scheduler.run_task(
            self, lambda: self.teleport_to_match(player, kit, map_data), delay=1
        )
    
    def teleport_to_match(self, player, kit: Dict, map_data: Dict) -> None:
        """Teleport player to match"""
        player_uuid = str(player.unique_id)
        data = self.get_player_data(player_uuid)
        
        # Reset inventory
        player.inventory.clear()
        
        # Teleport to map spawn
        spawn = map_data.get("spawn", {})
        level = self.server.get_level(map_data.get("world", "world"))
        if level:
            player.teleport(level, spawn.get("x", 0), spawn.get("y", 64), spawn.get("z", 0))
        
        # Set state to MATCH
        data.state = GameState.MATCH
        data.current_kit = kit.get("name")
        data.current_map = map_data.get("name")
        
        # Initialize rollback system
        self.init_rollback(player_uuid)
        
        # Show match info
        player.send_message(f"{ColorFormat.GOLD}=== FortCore ==={ColorFormat.RESET}")
        player.send_message(f"{ColorFormat.AQUA}{map_data.get('name')} {ColorFormat.GRAY}— By: {map_data.get('creator')}{ColorFormat.RESET}")
        player.send_message(f"{ColorFormat.YELLOW}{kit.get('name')} {ColorFormat.GRAY}— By: {kit.get('creator')}{ColorFormat.RESET}")
    
    def init_rollback(self, player_uuid: str) -> None:
        """Initialize rollback system for player"""
        data = self.get_player_data(player_uuid)
        
        # Clear existing buffer
        data.rollback_buffer.clear()
        
        # Create CSV file
        csv_path = self.rollback_dir / f"rollback_{player_uuid}.csv"
        data.csv_path = csv_path
        
        # Write header
        with open(csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "action", "x", "y", "z", "block_type"])
        
        data.last_flush = datetime.now().timestamp()
    
    @event_handler
    def on_block_break(self, event: BlockBreakEvent) -> None:
        """Record block breaks"""
        player = event.player
        player_uuid = str(player.unique_id)
        data = self.get_player_data(player_uuid)
        
        if data.state != GameState.MATCH:
            return
        
        block = event.block
        action = RollbackAction(
            "break",
            block.x,
            block.y,
            block.z,
            block.type,
            datetime.now().timestamp()
        )
        data.rollback_buffer.append(action)
    
    @event_handler
    def on_block_place(self, event: BlockPlaceEvent) -> None:
        """Record block placements"""
        player = event.player
        player_uuid = str(player.unique_id)
        data = self.get_player_data(player_uuid)
        
        if data.state != GameState.MATCH:
            return
        
        block = event.block
        action = RollbackAction(
            "place",
            block.x,
            block.y,
            block.z,
            block.type,
            datetime.now().timestamp()
        )
        data.rollback_buffer.append(action)
    
    def flush_all_buffers(self) -> None:
        """Flush all player buffers to disk (runs every 60 seconds)"""
        for player_uuid, data in self.player_data.items():
            if data.state == GameState.MATCH and data.rollback_buffer:
                self.flush_buffer(player_uuid)
    
    def flush_buffer(self, player_uuid: str) -> None:
        """Flush single player buffer to CSV"""
        data = self.get_player_data(player_uuid)
        
        if not data.csv_path or not data.rollback_buffer:
            return
        
        try:
            with open(data.csv_path, 'a', newline='') as f:
                writer = csv.writer(f)
                for action in data.rollback_buffer:
                    writer.writerow([
                        action.timestamp,
                        action.action_type,
                        action.x,
                        action.y,
                        action.z,
                        action.block_type
                    ])
            
            # Clear buffer after flush
            data.rollback_buffer.clear()
            data.last_flush = datetime.now().timestamp()
            
        except Exception as e:
            self.logger.error(f"Error flushing buffer for {player_uuid}: {e}")
    
    @event_handler
    def on_player_death(self, event: PlayerDeathEvent) -> None:
        """Handle player death"""
        player = event.entity
        player_uuid = str(player.unique_id)
        data = self.get_player_data(player_uuid)
        
        if data.state == GameState.MATCH:
            # Strike lightning at death location
            level = player.location.level
            level.strike_lightning(player.location)
        
        # Clear inventory
        player.inventory.clear()
        
        # Start rollback
        self.start_rollback(player_uuid)
    
    @event_handler
    def on_player_quit(self, event: PlayerQuitEvent) -> None:
        """Handle player disconnect"""
        player = event.player
        player_uuid = str(player.unique_id)
        data = self.get_player_data(player_uuid)
        
        if data.state == GameState.MATCH:
            # Flush buffer immediately
            self.flush_buffer(player_uuid)
            # Start rollback
            self.start_rollback(player_uuid)
    
    def on_command(self, sender: CommandSender, command: Command, args: list[str]) -> bool:
        """Handle /out command"""
        if command.name == "out":
            if not hasattr(sender, 'unique_id'):
                sender.send_message(f"{ColorFormat.RED}Only players can use this command!{ColorFormat.RESET}")
                return True
            
            player_uuid = str(sender.unique_id)
            data = self.get_player_data(player_uuid)
            
            if data.state != GameState.MATCH:
                sender.send_message(f"{ColorFormat.RED}You are not in a match!{ColorFormat.RESET}")
                return True
            
            # Flush buffer and start rollback
            self.flush_buffer(player_uuid)
            self.start_rollback(player_uuid)
            
            sender.send_message(f"{ColorFormat.YELLOW}Leaving match...{ColorFormat.RESET}")
            return True
        
        return False
    
    def start_rollback(self, player_uuid: str) -> None:
        """Start the rollback process"""
        data = self.get_player_data(player_uuid)
        
        if data.state == GameState.ROLLBACK:
            return  # Already rolling back
        
        # Flush any remaining buffer
        self.flush_buffer(player_uuid)
        
        # Set state
        data.state = GameState.ROLLBACK
        
        # Read CSV and start scheduled rollback
        if data.csv_path and data.csv_path.exists():
            actions = self.read_rollback_csv(data.csv_path)
            if actions:
                # Schedule rollback task (2 actions every 0.5 seconds = 10 ticks)
                task_id = self.server.scheduler.run_task_timer(
                    self, 
                    lambda: self.process_rollback_batch(player_uuid, actions),
                    delay=10,
                    period=10
                )
                self.rollback_tasks[player_uuid] = task_id
            else:
                self.finish_rollback(player_uuid)
        else:
            self.finish_rollback(player_uuid)
    
    def read_rollback_csv(self, csv_path: Path) -> List[Dict]:
        """Read rollback actions from CSV in reverse order"""
        actions = []
        try:
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                actions = list(reader)
                actions.reverse()  # Read from last to first
        except Exception as e:
            self.logger.error(f"Error reading rollback CSV: {e}")
        return actions
    
    def process_rollback_batch(self, player_uuid: str, actions: List[Dict]) -> None:
        """Process 2 rollback actions"""
        if not actions:
            self.finish_rollback(player_uuid)
            return
        
        # Process 2 actions
        for _ in range(min(2, len(actions))):
            if not actions:
                break
            
            action = actions.pop(0)
            self.revert_action(action)
        
        # If no more actions, finish
        if not actions:
            self.finish_rollback(player_uuid)
    
    def revert_action(self, action: Dict) -> None:
        """Revert a single action"""
        try:
            x = int(action["x"])
            y = int(action["y"])
            z = int(action["z"])
            block_type = action["block_type"]
            action_type = action["action"]
            
            # Get the level (assuming default world for now)
            level = self.server.get_level(self.config.get("lobby_spawn", {}).get("world", "world"))
            if not level:
                return
            
            block = level.get_block_at(x, y, z)
            
            if action_type == "place":
                # Player placed block, revert to air
                block.type = "minecraft:air"
            elif action_type == "break":
                # Player broke block, restore original
                block.type = block_type
                
        except Exception as e:
            self.logger.error(f"Error reverting action: {e}")
    
    def finish_rollback(self, player_uuid: str) -> None:
        """Finish rollback and reset player"""
        data = self.get_player_data(player_uuid)
        
        # Cancel rollback task
        if player_uuid in self.rollback_tasks:
            self.server.scheduler.cancel_task(self.rollback_tasks[player_uuid])
            del self.rollback_tasks[player_uuid]
        
        # Delete CSV file
        if data.csv_path and data.csv_path.exists():
            try:
                data.csv_path.unlink()
            except Exception as e:
                self.logger.error(f"Error deleting rollback CSV: {e}")
        
        # Clear data
        data.rollback_buffer.clear()
        data.csv_path = None
        data.current_kit = None
        data.current_map = None
        
        # Reset player
        player = self.server.get_player(uuid_lib.UUID(player_uuid))
        if player:
            self.reset_player(player)
        
        # Set state back to LOBBY
        data.state = GameState.LOBBY