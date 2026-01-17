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
import uuid as uuid_module

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
        self.plugin_config: Dict = {}
        self.teleport_cooldown: Dict[str, float] = {}
        self.rollback_dir: Path = None
        self.flush_task = None
        self.rollback_tasks: Dict[str, int] = {}
        
    def on_load(self) -> None:
        self.logger.info("FortCore loading...")
        self.load_plugin_config()
        
    def on_enable(self) -> None:
        self.logger.info("FortCore enabled!")
        self.register_events(self)
        self.register_command()
        
        self.rollback_dir = Path(self.data_folder) / "rollbacks"
        self.rollback_dir.mkdir(parents=True, exist_ok=True)
        
        self.server.scheduler.run_task(self, self.resume_incomplete_rollbacks, delay=40)
        self.flush_task = self.server.scheduler.run_task(self, self.flush_all_buffers, delay=1200, period=1200)
        
        self.logger.info(f"Loaded {len(self.plugin_config.get('maps', []))} maps and {len(self.plugin_config.get('kits', []))} kits")
        
    def on_disable(self) -> None:
        self.logger.info("FortCore disabling...")
        self.flush_all_buffers()
        if self.flush_task:
            try:
                self.server.scheduler.cancel_task(self.flush_task)
            except:
                pass
        for task_id in list(self.rollback_tasks.values()):
            try:
                self.server.scheduler.cancel_task(task_id)
            except:
                pass
    
    def resume_incomplete_rollbacks(self) -> None:
        """Resume any incomplete rollbacks from server restart"""
        try:
            if not self.rollback_dir.exists():
                return
            
            csv_files = list(self.rollback_dir.glob("rollback_*.csv"))
            if not csv_files:
                return
            
            self.logger.info(f"Found {len(csv_files)} incomplete rollback(s) from previous session")
            
            for csv_file in csv_files:
                try:
                    uuid_str = csv_file.stem.replace("rollback_", "")
                    
                    with open(csv_file, 'r') as f:
                        lines = f.readlines()
                    
                    if len(lines) <= 1:
                        csv_file.unlink()
                        self.logger.info(f"Deleted empty rollback file: {csv_file.name}")
                        continue
                    
                    self.logger.info(f"Resuming rollback for player {uuid_str}")
                    
                    data = self.get_player_data(uuid_str)
                    data.csv_path = csv_file
                    data.state = GameState.ROLLBACK
                    
                    actions = self.read_rollback_csv(csv_file)
                    if actions:
                        data.pending_rollback_actions = actions
                        task_id = self.server.scheduler.run_task(
                            self, 
                            lambda uid=uuid_str: self.process_rollback_batch(uid),
                            delay=10,
                            period=10
                        )
                        self.rollback_tasks[uuid_str] = task_id
                    else:
                        csv_file.unlink()
                        self.logger.info(f"Deleted invalid rollback file: {csv_file.name}")
                        
                except Exception as e:
                    self.logger.error(f"Error processing {csv_file.name}: {e}")
                    try:
                        csv_file.unlink()
                    except:
                        pass
                        
        except Exception as e:
            self.logger.error(f"Error during rollback resume: {e}")
    
    def register_command(self) -> None:
        """Register the /out command"""
        try:
            command = self.get_command("out")
            if command:
                command.executor = self
                perm = self.server.plugin_manager.add_permission("fortcore.command.out")
                if perm:
                    perm.description = "Allow players to leave matches"
                    perm.default = True
                self.logger.info("Registered /out command")
        except Exception as e:
            self.logger.error(f"Failed to register command: {e}")
        
    def load_plugin_config(self) -> None:
        """Load configuration from config.yml"""
        config_path = Path(self.data_folder) / "config.yml"
        
        if not config_path.exists():
            default_config = {
                "lobby_spawn": {"x": 0, "y": 100, "z": 0, "world": "world"},
                "maps": [{"name": "Diamond Arena", "creator": "Admin", "spawn": {"x": 100, "y": 64, "z": 100}, "world": "world"}],
                "kits": [{"name": "Diamond SMP", "creator": "Admin", "maxPlayers": 8}]
            }
            config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(config_path, 'w') as f:
                yaml.dump(default_config, f, default_flow_style=False)
            self.plugin_config = default_config
        else:
            with open(config_path, 'r') as f:
                self.plugin_config = yaml.safe_load(f)
                
    def get_player_data(self, player_uuid: str) -> PlayerData:
        """Get or create player data"""
        if player_uuid not in self.player_data:
            self.player_data[player_uuid] = PlayerData(player_uuid)
        return self.player_data[player_uuid]
    
    def reset_player(self, player) -> None:
        """Complete player reset - Returns player to lobby"""
        try:
            player_uuid = str(player.unique_id)
            data = self.get_player_data(player_uuid)
            
            if data.state != GameState.ROLLBACK:
                if player_uuid in self.rollback_tasks:
                    try:
                        task_id = self.rollback_tasks[player_uuid]
                        self.server.scheduler.cancel_task(task_id)
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
                data.current_kit = None
                data.current_map = None
            
            data.state = GameState.LOBBY
            player.game_mode = GameMode.SURVIVAL
            
            try:
                self.server.dispatch_command(self.server.command_sender, f'effect "{player.name}" clear')
            except Exception as e:
                self.logger.error(f"Failed to clear effects: {e}")
            
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
            
            lobby = self.plugin_config.get("lobby_spawn", {})
            
            # Always use player's current dimension
            dimension = player.location.dimension
            x = float(lobby.get("x", 0))
            y = float(lobby.get("y", 100))
            z = float(lobby.get("z", 0))
            
            from endstone import Location
            new_location = Location(dimension, x, y, z)
            player.teleport(new_location)
            
            from endstone.inventory import ItemStack
            menu_item = ItemStack("minecraft:lodestone_compass", 1)
            inventory.set_item(8, menu_item)
            
            try:
                self.server.dispatch_command(self.server.command_sender, f'effect "{player.name}" weakness 999999 255 true')
                self.server.dispatch_command(self.server.command_sender, f'effect "{player.name}" resistance 5 255 true')
                self.server.dispatch_command(self.server.command_sender, f'effect "{player.name}" blindness 5 255 true')
            except Exception as e:
                self.logger.error(f"Failed to apply effects: {e}")
            
        except Exception as e:
            self.logger.error(f"Error resetting player: {e}")
    
    @event_handler
    def on_player_join(self, event: PlayerJoinEvent) -> None:
        """Handle player join"""
        player = event.player
        player_uuid = str(player.unique_id)
        data = self.get_player_data(player_uuid)
        
        if data.state == GameState.ROLLBACK:
            self.logger.info(f"Player {player.name} joined during rollback")
        else:
            csv_path = self.rollback_dir / f"rollback_{player_uuid}.csv"
            if csv_path.exists():
                try:
                    csv_path.unlink()
                except:
                    pass
        
        self.server.scheduler.run_task(self, lambda: self.handle_join_sequence(player), delay=10)
    
    def handle_join_sequence(self, player) -> None:
        """Handle join sequence"""
        player_uuid = str(player.unique_id)
        data = self.get_player_data(player_uuid)
        
        if data.state != GameState.ROLLBACK:
            self.reset_player(player)
            player.send_message(f"{ColorFormat.GOLD}=== FortCore ==={ColorFormat.RESET}")
            player.send_message(f"{ColorFormat.YELLOW}Right-click the compass to join a match!{ColorFormat.RESET}")
        else:
            player.send_message(f"{ColorFormat.YELLOW}Your previous session is being cleaned up...{ColorFormat.RESET}")
    
    @event_handler
    def on_player_interact(self, event: PlayerInteractEvent) -> None:
        """Handle compass click"""
        player = event.player
        item = player.inventory.item_in_main_hand
        
        if item and item.type == "minecraft:lodestone_compass":
            player_uuid = str(player.unique_id)
            data = self.get_player_data(player_uuid)
            
            if data.state != GameState.LOBBY:
                if data.state == GameState.ROLLBACK:
                    player.send_message(f"{ColorFormat.YELLOW}Please wait, cleaning up...{ColorFormat.RESET}")
                else:
                    player.send_message(f"{ColorFormat.RED}You must be in the lobby!{ColorFormat.RESET}")
                return
            
            self.open_kit_menu(player)
    
    def open_kit_menu(self, player) -> None:
        """Open kit selection menu"""
        form = ActionForm()
        form.title = "FortCore"
        
        kits = self.plugin_config.get("kits", [])
        callbacks = []
        
        for i, kit in enumerate(kits):
            online_count = sum(1 for pd in self.player_data.values() 
                             if pd.state == GameState.MATCH and pd.current_kit == kit.get("name"))
            max_players = kit.get("maxPlayers", 8)
            button_text = f"{kit.get('name', 'Unknown')} [{online_count}/{max_players}]"
            
            def make_callback(idx):
                return lambda p: self.handle_kit_select(p, idx)
            
            callbacks.append(make_callback(i))
            form.add_button(button_text, on_click=callbacks[-1])
        
        player.send_form(form)
    
    def handle_kit_select(self, player, kit_index: int) -> None:
        """Handle kit selection"""
        player_uuid = str(player.unique_id)
        data = self.get_player_data(player_uuid)
        
        kits = self.plugin_config.get("kits", [])
        maps = self.plugin_config.get("maps", [])
        
        if kit_index >= len(kits) or kit_index >= len(maps):
            player.send_message(f"{ColorFormat.RED}Invalid selection!{ColorFormat.RESET}")
            return
        
        kit = kits[kit_index]
        map_data = maps[kit_index]
        
        if data.state != GameState.LOBBY:
            player.send_message(f"{ColorFormat.RED}You must be in the lobby!{ColorFormat.RESET}")
            return
        
        online_count = sum(1 for pd in self.player_data.values() 
                         if pd.state == GameState.MATCH and pd.current_kit == kit.get("name"))
        if online_count >= kit.get("maxPlayers", 8):
            player.send_message(f"{ColorFormat.RED}This match is full!{ColorFormat.RESET}")
            return
        
        current_time = datetime.now().timestamp()
        last_teleport = self.teleport_cooldown.get(kit.get("name"), 0)
        if current_time - last_teleport < 5.0:
            player.send_message(f"{ColorFormat.RED}Someone just teleported! Wait...{ColorFormat.RESET}")
            return
        
        data.state = GameState.TELEPORTING
        self.teleport_cooldown[kit.get("name")] = current_time
        
        self.server.scheduler.run_task(self, lambda: self.teleport_to_match(player, kit, map_data), delay=1)
    
    def teleport_to_match(self, player, kit: Dict, map_data: Dict) -> None:
        """Teleport player to match"""
        player_uuid = str(player.unique_id)
        data = self.get_player_data(player_uuid)
        
        player.inventory.clear()
        
        spawn = map_data.get("spawn", {})
        
        # Always use player's current dimension
        dimension = player.location.dimension
        x = float(spawn.get("x", 0))
        y = float(spawn.get("y", 64))
        z = float(spawn.get("z", 0))
        
        from endstone import Location
        new_location = Location(dimension, x, y, z)
        player.teleport(new_location)
        
        data.state = GameState.MATCH
        data.current_kit = kit.get("name")
        data.current_map = map_data.get("name")
        
        self.init_rollback(player_uuid)
        
        player.send_message(f"{ColorFormat.GOLD}=== FortCore ==={ColorFormat.RESET}")
        player.send_message(f"{ColorFormat.AQUA}{map_data.get('name')} {ColorFormat.GRAY}-- By: {map_data.get('creator')}{ColorFormat.RESET}")
        player.send_message(f"{ColorFormat.YELLOW}{kit.get('name')} {ColorFormat.GRAY}-- By: {kit.get('creator')}{ColorFormat.RESET}")
    
    def init_rollback(self, player_uuid: str) -> None:
        """Initialize rollback system"""
        data = self.get_player_data(player_uuid)
        data.rollback_buffer.clear()
        data.pending_rollback_actions.clear()
        
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
        action = RollbackAction("break", block.x, block.y, block.z, block.type, datetime.now().timestamp())
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
        action = RollbackAction("place", block.x, block.y, block.z, block.type, datetime.now().timestamp())
        data.rollback_buffer.append(action)
    
    def flush_all_buffers(self) -> None:
        """Flush all player buffers to disk"""
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
                    writer.writerow([action.timestamp, action.action_type, action.x, action.y, action.z, action.block_type])
            
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
        
        if data.state != GameState.MATCH:
            player.inventory.clear()
            self.server.scheduler.run_task(self, lambda: self.reset_player(player), delay=5)
            return
        
        try:
            dimension = player.location.dimension
            dimension.strike_lightning(player.location)
        except Exception as e:
            self.logger.error(f"Failed to strike lightning: {e}")
        
        player.inventory.clear()
        self.server.scheduler.run_task(self, lambda: self.start_rollback(player_uuid), delay=5)
    
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
                sender.send_message(f"{ColorFormat.RED}Only players can use this!{ColorFormat.RESET}")
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
        
        data.state = GameState.ROLLBACK
        self.flush_buffer(player_uuid)
        
        if data.csv_path and data.csv_path.exists():
            actions = self.read_rollback_csv(data.csv_path)
            if actions:
                data.pending_rollback_actions = actions
                task_id = self.server.scheduler.run_task(self, lambda: self.process_rollback_batch(player_uuid), delay=10, period=10)
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
            
            maps = self.plugin_config.get("maps", [])
            world_name = maps[0].get("world", "world") if maps else "world"
            
            try:
                level = self.server.get_world(world_name)
            except:
                try:
                    level = next((w for w in self.server.worlds if w.name == world_name), None)
                    if not level:
                        level = self.server.worlds[0] if self.server.worlds else None
                except:
                    level = None
            
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
        
        if data.state != GameState.ROLLBACK:
            return
        
        if player_uuid in self.rollback_tasks:
            try:
                task_id = self.rollback_tasks[player_uuid]
                self.server.scheduler.cancel_task(task_id)
            except Exception as e:
                self.logger.error(f"Error canceling task: {e}")
            finally:
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
        
        try:
            player = self.server.get_player(uuid_module.UUID(player_uuid))
        except:
            player = None
        
        if player:
            self.reset_player(player)
            player.send_message(f"{ColorFormat.GREEN}Rollback complete! You're back in the lobby.{ColorFormat.RESET}")
        else:
            data.state = GameState.LOBBY
