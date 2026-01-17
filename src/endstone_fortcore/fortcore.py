# FortCore - High-performance PvP Core Plugin for Endstone Bedrock Server
# Optimized for zero lag with async operations and efficient rollback system

from endstone.plugin import Plugin
from endstone.command import Command, CommandSender
from endstone.event import event_handler, PlayerJoinEvent, PlayerDeathEvent, PlayerQuitEvent, PlayerInteractEvent, BlockBreakEvent, BlockPlaceEvent
from endstone import ColorFormat, GameMode
from endstone.form import ActionForm
import yaml
import csv
from pathlib import Path
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional
import uuid

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
        self.current_map: Optional[str] = None
        self.current_kit: Optional[str] = None
        self.pending_rollback_actions: List[Dict] = []

class FortCore(Plugin):
    api_version = "0.5"
    
    def __init__(self):
        super().__init__()
        self.player_data: Dict[str, PlayerData] = {}
        self.plugin_config: Dict = {}  # Changed from self.config
        self.teleport_cooldown: Dict[str, float] = {}
        self.rollback_dir: Path = None
        self.flush_task = None
        self.rollback_tasks: Dict[str, int] = {}
        
    def on_load(self) -> None:
        self.logger.info("FortCore loading...")
        self.load_plugin_config()  # Changed method name
        
    def on_enable(self) -> None:
        self.logger.info("FortCore enabled!")
        self.register_events(self)
        
        # Register command
        self.register_command()
        
        # Create rollback directory
        self.rollback_dir = Path(self.data_folder) / "rollbacks"
        self.rollback_dir.mkdir(parents=True, exist_ok=True)
        
        # Start flush task every 60 seconds (1200 ticks)
        self.flush_task = self.server.scheduler.run_task(
            self, self.flush_all_buffers, delay=1200, period=1200
        )
        
        self.logger.info(f"Loaded {len(self.plugin_config.get('maps', []))} maps and {len(self.plugin_config.get('kits', []))} kits")
        
    def on_disable(self) -> None:
        self.logger.info("FortCore disabling...")
        self.flush_all_buffers()
        if self.flush_task:
            self.server.scheduler.cancel_task(self.flush_task)
        for task_id in self.rollback_tasks.values():
            self.server.scheduler.cancel_task(task_id)
    
    def register_command(self) -> None:
        """Register the /out command with permission for all players"""
        try:
            # Get the command
            command = self.get_command("out")
            if command:
                command.executor = self
                
                # Create permission for the command (accessible to everyone)
                perm = self.server.plugin_manager.add_permission("fortcore.command.out")
                if perm:
                    perm.description = "Allow players to leave matches"
                    perm.default = True  # Make it available to all players
                
                self.logger.info("Registered /out command")
        except Exception as e:
            self.logger.error(f"Failed to register command: {e}")
        
    def load_plugin_config(self) -> None:  # Renamed from load_config
        """Load configuration from config.yml"""
        config_path = Path(self.data_folder) / "config.yml"
        
        if not config_path.exists():
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
            self.plugin_config = default_config  # Changed
        else:
            with open(config_path, 'r') as f:
                self.plugin_config = yaml.safe_load(f)  # Changed
                
    def get_player_data(self, player_uuid: str) -> PlayerData:
        """Get or create player data"""
        if player_uuid not in self.player_data:
            self.player_data[player_uuid] = PlayerData(player_uuid)
        return self.player_data[player_uuid]
    
    def reset_player(self, player) -> None:
        """Complete player reset"""
        try:
            # Gamemode survival
            player.game_mode = GameMode.SURVIVAL
            
            # Clear all effects
            for effect in player.active_effects:
                player.remove_effect(effect.type)
            
            # Clear inventory (main, armor, offhand)
            inventory = player.inventory
            inventory.clear()
            
            # Teleport to lobby
            lobby = self.plugin_config.get("lobby_spawn", {})  # Changed
            level = self.server.get_level(lobby.get("world", "world"))
            if level:
                player.teleport(level, lobby.get("x", 0), lobby.get("y", 100), lobby.get("z", 0))
            
            # Give menu item (lodestone compass) in slot 9
            from endstone.inventory import ItemStack
            menu_item = ItemStack("minecraft:lodestone_compass", 1)
            inventory.set_item(8, menu_item)
            
            # Apply weakness 255 infinite no particles
            from endstone.potion import PotionEffect, PotionEffectType
            weakness = PotionEffect(PotionEffectType.WEAKNESS, -1, 255, False, False, False)
            player.add_effect(weakness)
            
        except Exception as e:
            self.logger.error(f"Error resetting player: {e}")
    
    @event_handler
    def on_player_join(self, event: PlayerJoinEvent) -> None:
        """Handle player join"""
        player = event.player
        self.server.scheduler.run_task(
            self, lambda: self.handle_join_sequence(player), delay=10
        )
    
    def handle_join_sequence(self, player) -> None:
        """Handle join sequence"""
        player_uuid = str(player.unique_id)
        data = self.get_player_data(player_uuid)
        
        # Reset player
        self.reset_player(player)
        
        # Set state to LOBBY
        data.state = GameState.LOBBY
        
        # Welcome message
        player.send_message(f"{ColorFormat.GOLD}=== FortCore ==={ColorFormat.RESET}")
        player.send_message(f"{ColorFormat.YELLOW}Right-click the compass to join a match!{ColorFormat.RESET}")
    
    @event_handler
    def on_player_interact(self, event: PlayerInteractEvent) -> None:
        """Handle compass click to open menu"""
        player = event.player
        item = player.inventory.item_in_main_hand
        
        if item and item.type == "minecraft:lodestone_compass":
            self.open_kit_menu(player)
    
    def open_kit_menu(self, player) -> None:
        """Open the kit selection menu"""
        form = ActionForm()
        form.title = "FortCore"
        
        kits = self.plugin_config.get("kits", [])  # Changed
        
        for i, kit in enumerate(kits):
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
        
        kits = self.plugin_config.get("kits", [])  # Changed
        maps = self.plugin_config.get("maps", [])  # Changed
        
        if kit_index >= len(kits) or kit_index >= len(maps):
            player.send_message(f"{ColorFormat.RED}Invalid selection!{ColorFormat.RESET}")
            return
        
        kit = kits[kit_index]
        map_data = maps[kit_index]
        
        # Check if already in match
        if data.state == GameState.MATCH or data.state == GameState.TELEPORTING:
            player.send_message(f"{ColorFormat.RED}You are already in a match!{ColorFormat.RESET}")
            return
        
        # Check if kit is full
        online_count = sum(1 for pd in self.player_data.values() 
                         if pd.state == GameState.MATCH and pd.current_kit == kit.get("name"))
        if online_count >= kit.get("maxPlayers", 8):
            player.send_message(f"{ColorFormat.RED}This match is full!{ColorFormat.RESET}")
            return
        
        # Check global cooldown (5 seconds)
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
        
        # Clear inventory
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
        
        # Initialize rollback
        self.init_rollback(player_uuid)
        
        # Show match info
        player.send_message(f"{ColorFormat.GOLD}=== FortCore ==={ColorFormat.RESET}")
        player.send_message(f"{ColorFormat.AQUA}{map_data.get('name')} {ColorFormat.GRAY}-- By: {map_data.get('creator')}{ColorFormat.RESET}")
        player.send_message(f"{ColorFormat.YELLOW}{kit.get('name')} {ColorFormat.GRAY}-- By: {kit.get('creator')}{ColorFormat.RESET}")
    
    def init_rollback(self, player_uuid: str) -> None:
        """Initialize rollback system"""
        data = self.get_player_data(player_uuid)
        
        data.rollback_buffer.clear()
        
        csv_path = self.rollback_dir / f"rollback_{player_uuid}.csv"
        data.csv_path = csv_path
        
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
        """Flush all player buffers to disk (every 60 seconds)"""
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
            
            data.rollback_buffer.clear()
            data.last_flush = datetime.now().timestamp()
            
        except Exception as e:
            self.logger.error(f"Error flushing buffer: {e}")
    
    @event_handler
    def on_player_death(self, event: PlayerDeathEvent) -> None:
        """Handle player death"""
        player = event.player
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
            self.flush_buffer(player_uuid)
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
            
            self.flush_buffer(player_uuid)
            self.start_rollback(player_uuid)
            
            sender.send_message(f"{ColorFormat.YELLOW}Leaving match...{ColorFormat.RESET}")
            return True
        
        return False
    
    def start_rollback(self, player_uuid: str) -> None:
        """Start the rollback process"""
        data = self.get_player_data(player_uuid)
        
        if data.state == GameState.ROLLBACK:
            return
        
        self.flush_buffer(player_uuid)
        data.state = GameState.ROLLBACK
        
        if data.csv_path and data.csv_path.exists():
            actions = self.read_rollback_csv(data.csv_path)
            if actions:
                # Store actions in player data so the task can access them
                data.pending_rollback_actions = actions
                task_id = self.server.scheduler.run_task(
                    self, 
                    lambda: self.process_rollback_batch(player_uuid),
                    delay=10,
                    period=10
                )
                self.rollback_tasks[player_uuid] = task_id
            else:
                self.finish_rollback(player_uuid)
        else:
            self.finish_rollback(player_uuid)
    
    def read_rollback_csv(self, csv_path: Path) -> List[Dict]:
        """Read rollback actions from CSV in reverse"""
        actions = []
        try:
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                actions = list(reader)
                actions.reverse()
        except Exception as e:
            self.logger.error(f"Error reading CSV: {e}")
        return actions
    
    def process_rollback_batch(self, player_uuid: str) -> None:
        """Process 2 rollback actions every 0.5 seconds"""
        data = self.get_player_data(player_uuid)
        actions = data.pending_rollback_actions
        
        if not actions:
            self.finish_rollback(player_uuid)
            return
        
        for _ in range(min(2, len(actions))):
            if not actions:
                break
            
            action = actions.pop(0)
            self.revert_action(action)
        
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
            
            level = self.server.get_level(self.plugin_config.get("lobby_spawn", {}).get("world", "world"))  # Changed
            if not level:
                return
            
            block = level.get_block_at(x, y, z)
            
            if action_type == "place":
                block.type = "minecraft:air"
            elif action_type == "break":
                block.type = block_type
                
        except Exception as e:
            self.logger.error(f"Error reverting action: {e}")
    
    def finish_rollback(self, player_uuid: str) -> None:
        """Finish rollback and reset player"""
        data = self.get_player_data(player_uuid)
        
        if player_uuid in self.rollback_tasks:
            self.server.scheduler.cancel_task(self.rollback_tasks[player_uuid])
            del self.rollback_tasks[player_uuid]
        
        if data.csv_path and data.csv_path.exists():
            try:
                data.csv_path.unlink()
            except Exception as e:
                self.logger.error(f"Error deleting CSV: {e}")
        
        data.rollback_buffer.clear()
        data.pending_rollback_actions.clear()
        data.csv_path = None
        data.current_kit = None
        data.current_map = None
        
        # Use standard uuid.UUID instead of uuid_lib.UUID
        player = self.server.get_player(uuid.UUID(player_uuid))
        if player:
            self.reset_player(player)
        
        data.state = GameState.LOBBY
