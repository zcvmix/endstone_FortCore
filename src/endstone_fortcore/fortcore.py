# FortCore - High-performance PvP Core Plugin for Endstone Bedrock Server
# Production-ready with match tracking and spawn teleport

from endstone.plugin import Plugin
from endstone.command import Command, CommandSender
from endstone.event import event_handler, PlayerJoinEvent, PlayerDeathEvent, PlayerQuitEvent, PlayerInteractEvent, BlockBreakEvent, BlockPlaceEvent, PlayerRespawnEvent, PlayerDropItemEvent, PlayerMoveEvent
from endstone import ColorFormat, GameMode
from endstone.level import Location
from endstone.form import ActionForm
from endstone.inventory import ItemStack
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional

from endstone_fortcore.rollback import RollbackManager, GameState, PlayerData, RollbackAction

class FortCore(Plugin):
    api_version = "0.5"
    
    commands = {
        "spawn": {
            "description": "Return to lobby (hold still for 5 seconds)",
            "usages": ["/spawn"],
            "permissions": ["fortcore.command.spawn"]
        }
    }
    
    permissions = {
        "fortcore.command.spawn": {
            "description": "Allow returning to lobby",
            "default": True
        }
    }
    
    def __init__(self):
        super().__init__()
        self.player_data: Dict[str, PlayerData] = {}
        self.match_config: Dict = {}
        self.match_stats: Dict = {}
        self.teleport_cooldown: Dict[str, float] = {}
        self.menu_cooldown: Dict[str, float] = {}
        self.spawn_requests: Dict[str, Dict] = {}
        self.rollback_manager: Optional[RollbackManager] = None
        
    def on_load(self) -> None:
        self.logger.info("FortCore loading...")
        self.load_match_config()
        self.load_match_stats()
        
    def on_enable(self) -> None:
        self.logger.info("FortCore enabled!")
        self.register_events(self)
        
        rollback_dir = Path(self.data_folder) / "rollbacks"
        rollback_dir.mkdir(parents=True, exist_ok=True)
        
        self.rollback_manager = RollbackManager(self, rollback_dir)
        
        # Resume incomplete rollbacks
        self.server.scheduler.run_task(self, lambda: self.rollback_manager.resume_rollbacks(), delay=20)
        
        # Check spawn requests every second
        self.server.scheduler.run_task(self, self.check_spawn_requests, delay=20, period=20)
        
        total = sum(len(m) for m in self.match_config.get("categories", {}).values())
        self.logger.info(f"Loaded {len(self.match_config.get('categories', {}))} categories with {total} matches")
        
    def on_disable(self) -> None:
        self.logger.info("FortCore disabling...")
        if self.rollback_manager:
            self.rollback_manager.shutdown()
        self.save_match_stats()
    
    def load_match_config(self) -> None:
        """Load configuration from config.json"""
        config_path = Path(self.data_folder) / "config.json"
        
        if not config_path.exists():
            default_config = {
                "lobby_spawn": [0.5, 100.0, 0.5],
                "rollback_enabled": True,
                "categories": {
                    "SMP": {
                        "DiamondSMP": {
                            "map": "Diamond Arena",
                            "kit": "Diamond Kit",
                            "max_players": 8,
                            "spawn": [100.5, 64.0, 100.5],
                            "rollback_enabled": True
                        }
                    }
                }
            }
            config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(config_path, 'w') as f:
                json.dump(default_config, f, indent=2)
            self.match_config = default_config
        else:
            with open(config_path, 'r') as f:
                self.match_config = json.load(f)
    
    def load_match_stats(self) -> None:
        """Load match statistics from match_stats.json"""
        stats_path = Path(self.data_folder) / "match_stats.json"
        
        if not stats_path.exists():
            self.match_stats = {}
        else:
            with open(stats_path, 'r') as f:
                self.match_stats = json.load(f)
    
    def save_match_stats(self) -> None:
        """Save match statistics to match_stats.json"""
        stats_path = Path(self.data_folder) / "match_stats.json"
        with open(stats_path, 'w') as f:
            json.dump(self.match_stats, f, indent=2)
    
    def get_player_match_count(self, player_uuid: str) -> int:
        """Get total matches played by player"""
        return self.match_stats.get(player_uuid, {}).get("total_matches", 0)
    
    def increment_player_match_count(self, player_uuid: str) -> int:
        """Increment and return player's match count"""
        if player_uuid not in self.match_stats:
            self.match_stats[player_uuid] = {"total_matches": 0}
        
        self.match_stats[player_uuid]["total_matches"] += 1
        self.save_match_stats()
        return self.match_stats[player_uuid]["total_matches"]
    
    def get_ordinal(self, n: int) -> str:
        """Convert number to ordinal (1st, 2nd, 3rd, etc.)"""
        if 10 <= n % 100 <= 20:
            suffix = 'th'
        else:
            suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
        return f"{n}{suffix}"
                
    def get_player_data(self, player_uuid: str) -> PlayerData:
        """Get or create player data"""
        if player_uuid not in self.player_data:
            self.player_data[player_uuid] = PlayerData(player_uuid)
        return self.player_data[player_uuid]
    
    def get_match_player_count(self, category: str, match_name: str) -> int:
        """Get actual number of players in a specific match"""
        count = 0
        for pd in self.player_data.values():
            if (pd.state == GameState.MATCH and 
                pd.current_category == category and 
                pd.current_match == match_name):
                count += 1
        return count
    
    def get_category_player_count(self, category: str) -> int:
        """Get total number of players in a category"""
        matches = self.match_config.get("categories", {}).get(category, {})
        total = 0
        for match_name in matches.keys():
            total += self.get_match_player_count(category, match_name)
        return total
    
    def get_category_max_players(self, category: str) -> int:
        """Get total max players for a category"""
        matches = self.match_config.get("categories", {}).get(category, {})
        return sum(m.get("max_players", 8) for m in matches.values())
    
    def reset_player(self, player) -> None:
        """Complete player reset"""
        try:
            player_uuid = str(player.unique_id)
            data = self.get_player_data(player_uuid)
            
            # Clear current match/category
            data.current_category = None
            data.current_match = None
            data.state = GameState.LOBBY
            
            player.game_mode = GameMode.SURVIVAL
            
            try:
                self.server.dispatch_command(self.server.command_sender, f'effect "{player.name}" clear')
            except:
                pass
            
            inventory = player.inventory
            inventory.clear()
            
            try:
                for i in range(4):
                    inventory.set_armor_contents(i, None)
            except:
                pass
            
            try:
                inventory.set_item_in_off_hand(None)
            except:
                pass
            
            lobby = self.match_config.get("lobby_spawn", [0.5, 100.0, 0.5])
            x, y, z = float(lobby[0]), float(lobby[1]), float(lobby[2])
            
            new_location = Location(player.location.dimension, x, y, z)
            player.teleport(new_location)
            
            try:
                self.server.dispatch_command(
                    self.server.command_sender,
                    f'give "{player.name}" lodestone_compass 1 0 {{"minecraft:item_lock":{{"mode":"lock_in_inventory"}}}}'
                )
            except:
                menu_item = ItemStack("minecraft:lodestone_compass", 1)
                inventory.set_item(8, menu_item)
            
            try:
                self.server.dispatch_command(self.server.command_sender, f'effect "{player.name}" weakness 999999 255 true')
                self.server.dispatch_command(self.server.command_sender, f'effect "{player.name}" resistance 5 255 true')
                self.server.dispatch_command(self.server.command_sender, f'effect "{player.name}" blindness 5 255 true')
            except:
                pass
            
        except Exception as e:
            self.logger.error(f"Error resetting player: {e}")
    
    @event_handler
    def on_player_join(self, event: PlayerJoinEvent) -> None:
        """Handle player join"""
        player = event.player
        player_uuid = str(player.unique_id)
        
        # Force clear existing data
        if player_uuid in self.player_data:
            old_data = self.player_data[player_uuid]
            old_data.current_category = None
            old_data.current_match = None
            old_data.state = GameState.LOBBY
        
        data = self.get_player_data(player_uuid)
        
        if data.state == GameState.ROLLBACK:
            self.logger.info(f"Player {player.name} joined during rollback")
        
        self.server.scheduler.run_task(self, lambda: self.handle_join_sequence(player), delay=10)
    
    def handle_join_sequence(self, player) -> None:
        """Handle join sequence"""
        player_uuid = str(player.unique_id)
        data = self.get_player_data(player_uuid)
        
        self.reset_player(player)
        player.send_message(f"{ColorFormat.GOLD}=== FortCore ==={ColorFormat.RESET}")
        
        if data.state == GameState.ROLLBACK:
            player.send_message(f"{ColorFormat.YELLOW}Cleaning up your previous session...{ColorFormat.RESET}")
        
        player.send_message(f"{ColorFormat.YELLOW}Right-click the compass to select a match!{ColorFormat.RESET}")
    
    @event_handler
    def on_player_respawn(self, event: PlayerRespawnEvent) -> None:
        """Handle player respawn"""
        player = event.player
        self.server.scheduler.run_task(self, lambda: self.handle_respawn(player), delay=5)
    
    def handle_respawn(self, player) -> None:
        """Handle respawn sequence"""
        lobby = self.match_config.get("lobby_spawn", [0.5, 100.0, 0.5])
        x, y, z = float(lobby[0]), float(lobby[1]), float(lobby[2])
        
        new_location = Location(player.location.dimension, x, y, z)
        player.teleport(new_location)
        
        try:
            self.server.dispatch_command(
                self.server.command_sender,
                f'give "{player.name}" lodestone_compass 1 0 {{"minecraft:item_lock":{{"mode":"lock_in_inventory"}}}}'
            )
        except:
            menu_item = ItemStack("minecraft:lodestone_compass", 1)
            player.inventory.set_item(8, menu_item)
    
    @event_handler
    def on_player_drop_item(self, event: PlayerDropItemEvent) -> None:
        """Prevent dropping compass"""
        if event.item_drop.item_stack.type == "minecraft:lodestone_compass":
            event.cancelled = True
    
    @event_handler
    def on_player_interact(self, event: PlayerInteractEvent) -> None:
        """Handle compass click"""
        player = event.player
        item = player.inventory.item_in_main_hand
        
        if item and item.type == "minecraft:lodestone_compass":
            player_uuid = str(player.unique_id)
            data = self.get_player_data(player_uuid)
            
            if data.state != GameState.LOBBY:
                return
            
            current_time = datetime.now().timestamp()
            last_open = self.menu_cooldown.get(player_uuid, 0)
            
            if current_time - last_open < 1.0:
                return
            
            self.menu_cooldown[player_uuid] = current_time
            self.open_category_menu(player)
    
    def open_category_menu(self, player) -> None:
        """Open category selection menu"""
        form = ActionForm()
        form.title = "FortCore - Select Category"
        
        categories = self.match_config.get("categories", {})
        
        for category_name in categories.keys():
            online = self.get_category_player_count(category_name)
            max_p = self.get_category_max_players(category_name)
            
            pct = (online / max_p * 100) if max_p > 0 else 0
            color = ColorFormat.RED if pct >= 90 else (ColorFormat.GOLD if pct >= 50 else ColorFormat.GREEN)
            
            button_text = f"{category_name} {color}[{online}/{max_p}]{ColorFormat.RESET}"
            
            def make_callback(cat):
                return lambda p: self.open_match_menu(p, cat)
            
            form.add_button(button_text, on_click=make_callback(category_name))
        
        form.on_close = lambda p: None
        player.send_form(form)
    
    def open_match_menu(self, player, category: str) -> None:
        """Open match selection menu"""
        form = ActionForm()
        form.title = f"FortCore - {category}"
        
        matches = self.match_config.get("categories", {}).get(category, {})
        
        for match_name, match_data in matches.items():
            online = self.get_match_player_count(category, match_name)
            max_p = match_data.get("max_players", 8)
            
            pct = (online / max_p * 100) if max_p > 0 else 0
            color = ColorFormat.RED if pct >= 90 else (ColorFormat.GOLD if pct >= 50 else ColorFormat.GREEN)
            
            button_text = f"{match_name} {color}[{online}/{max_p}]{ColorFormat.RESET}"
            
            def make_callback(cat, match):
                return lambda p: self.handle_match_select(p, cat, match)
            
            form.add_button(button_text, on_click=make_callback(category, match_name))
        
        form.on_close = lambda p: self.open_category_menu(p)
        player.send_form(form)
    
    def handle_match_select(self, player, category: str, match_name: str) -> None:
        """Handle match selection"""
        player_uuid = str(player.unique_id)
        data = self.get_player_data(player_uuid)
        
        match_data = self.match_config.get("categories", {}).get(category, {}).get(match_name)
        
        if not match_data:
            player.send_message(f"{ColorFormat.RED}Invalid match!{ColorFormat.RESET}")
            return
        
        if data.state != GameState.LOBBY:
            player.send_message(f"{ColorFormat.RED}You must be in lobby!{ColorFormat.RESET}")
            return
        
        online = self.get_match_player_count(category, match_name)
        if online >= match_data.get("max_players", 8):
            player.send_message(f"{ColorFormat.RED}Match is full!{ColorFormat.RESET}")
            return
        
        current_time = datetime.now().timestamp()
        last_tp = self.teleport_cooldown.get(f"{category}:{match_name}", 0)
        if current_time - last_tp < 5.0:
            player.send_message(f"{ColorFormat.RED}Someone just teleported! Wait...{ColorFormat.RESET}")
            return
        
        data.state = GameState.TELEPORTING
        self.teleport_cooldown[f"{category}:{match_name}"] = current_time
        
        self.server.scheduler.run_task(self, lambda: self.teleport_to_match(player, category, match_name, match_data), delay=1)
    
    def teleport_to_match(self, player, category: str, match_name: str, match_data: Dict) -> None:
        """Teleport player to match"""
        player_uuid = str(player.unique_id)
        data = self.get_player_data(player_uuid)
        
        player.inventory.clear()
        
        spawn = match_data.get("spawn", [0.5, 64.0, 0.5])
        x, y, z = float(spawn[0]), float(spawn[1]), float(spawn[2])
        
        new_location = Location(player.location.dimension, x, y, z)
        player.teleport(new_location)
        
        # Set state FIRST
        data.state = GameState.MATCH
        data.current_category = category
        data.current_match = match_name
        
        # Increment match count
        match_count = self.increment_player_match_count(player_uuid)
        ordinal = self.get_ordinal(match_count)
        
        # Check rollback settings
        global_rollback = self.match_config.get("rollback_enabled", True)
        match_rollback = match_data.get("rollback_enabled", True)
        
        if global_rollback and match_rollback:
            self.rollback_manager.init_rollback(player_uuid, data)
        else:
            data.rollback_enabled = False
        
        # Get current player count
        total_players = self.get_match_player_count(category, match_name)
        
        # Broadcast join message
        if total_players == 1:
            self.server.broadcast_message(f"{ColorFormat.GREEN}{player.name}{ColorFormat.YELLOW} joined {ColorFormat.AQUA}{match_name}{ColorFormat.RESET}")
        else:
            others = total_players - 1
            self.server.broadcast_message(f"{ColorFormat.GREEN}{player.name}{ColorFormat.YELLOW} and {ColorFormat.GOLD}{others}{ColorFormat.YELLOW} other{'s' if others > 1 else ''} joined {ColorFormat.AQUA}{match_name}{ColorFormat.RESET}")
        
        # Send player info
        player.send_message(f"{ColorFormat.GOLD}=== FortCore ==={ColorFormat.RESET}")
        player.send_message(f"{ColorFormat.AQUA}Category: {category}{ColorFormat.RESET}")
        player.send_message(f"{ColorFormat.YELLOW}Map: {match_data.get('map')}{ColorFormat.RESET}")
        player.send_message(f"{ColorFormat.GREEN}Kit: {match_data.get('kit')}{ColorFormat.RESET}")
        player.send_message(f"{ColorFormat.LIGHT_PURPLE}This is your {ordinal} match!{ColorFormat.RESET}")
    
    @event_handler
    def on_block_break(self, event: BlockBreakEvent) -> None:
        """Record block breaks"""
        player = event.player
        player_uuid = str(player.unique_id)
        data = self.get_player_data(player_uuid)
        
        if data.state != GameState.MATCH or not data.rollback_enabled:
            return
        
        block = event.block
        
        # Skip liquids
        if block.type in ["minecraft:water", "minecraft:lava", "minecraft:flowing_water", "minecraft:flowing_lava"]:
            return
        
        action = RollbackAction("break", block.x, block.y, block.z, block.type, datetime.now().timestamp())
        data.rollback_buffer.append(action)
        
        if len(data.rollback_buffer) >= 50:
            self.rollback_manager.flush_buffer(data)
    
    @event_handler
    def on_block_place(self, event: BlockPlaceEvent) -> None:
        """Record block placements"""
        player = event.player
        player_uuid = str(player.unique_id)
        data = self.get_player_data(player_uuid)
        
        if data.state != GameState.MATCH or not data.rollback_enabled:
            return
        
        block = event.block
        action = RollbackAction("place", block.x, block.y, block.z, block.type, datetime.now().timestamp())
        data.rollback_buffer.append(action)
        
        if len(data.rollback_buffer) >= 50:
            self.rollback_manager.flush_buffer(data)
    
    @event_handler
    def on_player_death(self, event: PlayerDeathEvent) -> None:
        """Handle player death"""
        player = event.player
        player_uuid = str(player.unique_id)
        data = self.get_player_data(player_uuid)
        
        if data.state != GameState.MATCH:
            player.inventory.clear()
            return
        
        # Strike lightning at death location
        try:
            x, y, z = player.location.x, player.location.y, player.location.z
            # Use execute command to avoid target errors
            self.server.dispatch_command(self.server.command_sender, f'execute @a[name="{player.name}"] ~~~ summon lightning_bolt ~~~')
        except:
            pass
        
        player.inventory.clear()
        
        if data.rollback_enabled:
            self.server.scheduler.run_task(self, lambda: self.rollback_manager.start_rollback(player_uuid, data, player), delay=5)
        else:
            self.server.scheduler.run_task(self, lambda: self.reset_player(player), delay=5)
    
    @event_handler
    def on_player_quit(self, event: PlayerQuitEvent) -> None:
        """Handle player disconnect"""
        player = event.player
        player_uuid = str(player.unique_id)
        data = self.get_player_data(player_uuid)
        
        # Cancel spawn request if exists
        if player_uuid in self.spawn_requests:
            del self.spawn_requests[player_uuid]
        
        if data.state == GameState.MATCH and data.rollback_enabled:
            self.rollback_manager.start_rollback(player_uuid, data, None)
        else:
            data.current_category = None
            data.current_match = None
            data.state = GameState.LOBBY
    
    @event_handler
    def on_player_move(self, event: PlayerMoveEvent) -> None:
        """Handle player movement - check spawn requests"""
        player = event.player
        player_uuid = str(player.unique_id)
        
        if player_uuid in self.spawn_requests:
            # Player moved, cancel spawn request
            del self.spawn_requests[player_uuid]
            player.send_message(f"{ColorFormat.RED}Spawn cancelled - you moved!{ColorFormat.RESET}")
    
    def check_spawn_requests(self) -> None:
        """Check spawn requests every second"""
        current_time = datetime.now().timestamp()
        completed = []
        
        for player_uuid, request in self.spawn_requests.items():
            if current_time - request["start_time"] >= 5.0:
                # 5 seconds passed, teleport player
                completed.append(player_uuid)
                
                try:
                    player = request["player"]
                    data = self.get_player_data(player_uuid)
                    
                    if data.rollback_enabled:
                        self.rollback_manager.start_rollback(player_uuid, data, player)
                    else:
                        self.reset_player(player)
                except Exception as e:
                    self.logger.error(f"Error completing spawn request: {e}")
        
        # Remove completed requests
        for player_uuid in completed:
            del self.spawn_requests[player_uuid]
    
    def on_command(self, sender: CommandSender, command: Command, args: list[str]) -> bool:
        """Handle /spawn command"""
        if command.name == "spawn":
            if not hasattr(sender, "unique_id"):
                sender.send_message("Only players can use this.")
                return True
            
            player_uuid = str(sender.unique_id)
            data = self.get_player_data(player_uuid)
            
            if data.state != GameState.MATCH:
                sender.send_message("You are not in a match!")
                return True
            
            # Start spawn request
            self.spawn_requests[player_uuid] = {
                "player": sender,
                "start_time": datetime.now().timestamp()
            }
            
            sender.send_message(f"{ColorFormat.YELLOW}Don't move for 5 seconds to return to spawn...{ColorFormat.RESET}")
            return True
        
        return False