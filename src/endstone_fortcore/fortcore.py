# FortCore - High-performance PvP Core Plugin for Endstone Bedrock Server
# Optimized: JSONL Storage, Fast Rollback (100 blocks/tick), Robust Dimension Handling

from endstone.plugin import Plugin
from endstone.command import Command, CommandSender
from endstone.event import (
    event_handler, 
    PlayerJoinEvent, 
    PlayerDeathEvent, 
    PlayerQuitEvent, 
    PlayerInteractEvent, 
    BlockBreakEvent, 
    BlockPlaceEvent, 
    PlayerRespawnEvent, 
    PlayerDropItemEvent
)
from endstone import ColorFormat, GameMode
from endstone.level import Location
from endstone.form import ActionForm
from endstone.inventory import ItemStack
import json
from pathlib import Path
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any
import uuid as uuid_module

# --- Configuration & State ---

class GameState(Enum):
    """Player game states"""
    LOBBY = "LOBBY"
    QUEUE = "QUEUE"
    TELEPORTING = "TELEPORTING"
    MATCH = "MATCH"
    ROLLBACK = "ROLLBACK" # State where map is resetting
    END = "END"

class PlayerData:
    """Stores player state and rollback data"""
    def __init__(self, uuid: str):
        self.uuid = uuid
        self.state = GameState.LOBBY
        self.rollback_buffer: List[Dict[str, Any]] = [] # Buffer for RAM
        self.json_path: Optional[Path] = None           # Path to .jsonl file
        self.last_flush = datetime.now().timestamp()
        self.current_category: Optional[str] = None
        self.current_match: Optional[str] = None
        self.pending_rollback_actions: List[Dict] = []
        self.world_name: Optional[str] = None 

class FortCore(Plugin):
    api_version = "0.5"
    
    def __init__(self):
        super().__init__()
        self.player_data: Dict[str, PlayerData] = {}
        self.plugin_config: Dict = {}
        self.teleport_cooldown: Dict[str, float] = {}
        self.form_cooldown: Dict[str, float] = {}
        self.rollback_dir: Path = None
        self.flush_task = None
        self.rollback_tasks: Dict[str, int] = {}
        
    def on_load(self) -> None:
        self.logger.info("FortCore loading...")
        self.load_plugin_config()
        
    def on_enable(self) -> None:
        self.logger.info("FortCore enabled!")
        self.register_events(self)
        
        # Setup directories
        self.rollback_dir = Path(self.data_folder) / "rollbacks"
        self.rollback_dir.mkdir(parents=True, exist_ok=True)
        
        # Tasks
        self.server.scheduler.run_task(self, self.resume_incomplete_rollbacks, delay=40)
        # Flush RAM buffer to Disk every 60 seconds (1200 ticks)
        self.flush_task = self.server.scheduler.run_task(self, self.flush_all_buffers, delay=1200, period=1200)
        
        total_matches = sum(len(matches) for matches in self.plugin_config.get("categories", {}).values())
        self.logger.info(f"Loaded {len(self.plugin_config.get('categories', {}))} categories.")
        
    def on_disable(self) -> None:
        self.logger.info("FortCore disabling...")
        self.flush_all_buffers() # Save everything before shutdown
        
        # Cancel all tasks
        if self.flush_task:
            try: self.server.scheduler.cancel_task(self.flush_task)
            except: pass
            
        for task_id in list(self.rollback_tasks.values()):
            try: self.server.scheduler.cancel_task(task_id)
            except: pass
    
    def load_plugin_config(self) -> None:
        """Load configuration with safe defaults"""
        config_path = Path(self.data_folder) / "config.json"
        
        default_config = {
            "world_name": "minecraft:overworld",  # Use namespace for safety
            "lobby_spawn": [0.5, 100.0, 0.5],
            "categories": {
                "PvP": {
                    "Arena-1": {
                        "map": "Classic",
                        "kit": "Warrior",
                        "max_players": 2,
                        "spawn": [100.5, 64.0, 100.5]
                    }
                }
            }
        }

        if not config_path.exists():
            config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(config_path, 'w') as f:
                json.dump(default_config, f, indent=2)
            self.plugin_config = default_config
        else:
            with open(config_path, 'r') as f:
                self.plugin_config = json.load(f)
                
    def get_player_data(self, player_uuid: str) -> PlayerData:
        if player_uuid not in self.player_data:
            self.player_data[player_uuid] = PlayerData(player_uuid)
        return self.player_data[player_uuid]
    
    # --- Helper: Safe Dimension Getting ---
    def get_safe_dimension(self, dimension_name: str):
        """Safely retrieves a dimension, handling missing namespaces"""
        level = self.server.level
        if not level: return None
        
        # Try exact name first (e.g., "minecraft:overworld")
        dim = level.get_dimension(dimension_name)
        if dim: return dim
        
        # Try adding namespace if missing
        if ":" not in dimension_name:
            dim = level.get_dimension(f"minecraft:{dimension_name}")
            if dim: return dim
            
        # Fallback: Try "overworld" explicitly if all else fails
        return level.get_dimension("minecraft:overworld")

    # --- Match Logic ---
    
    @event_handler
    def on_player_join(self, event: PlayerJoinEvent) -> None:
        player = event.player
        self.server.scheduler.run_task(self, lambda: self.handle_join_sequence(player), delay=10)
    
    def handle_join_sequence(self, player) -> None:
        player_uuid = str(player.unique_id)
        data = self.get_player_data(player_uuid)
        
        # If they joined while a rollback file exists, we must process it
        json_path = self.rollback_dir / f"rollback_{player_uuid}.jsonl"
        if json_path.exists() and data.state != GameState.ROLLBACK:
             self.start_rollback(player_uuid) # Resume rollback
             return

        if data.state == GameState.ROLLBACK:
            player.send_message(f"{ColorFormat.YELLOW}Cleaning up your previous match...{ColorFormat.RESET}")
            player.game_mode = GameMode.SPECTATOR
        else:
            self.reset_player(player)

    def reset_player(self, player) -> None:
        """Send player to lobby and reset inventory"""
        try:
            player_uuid = str(player.unique_id)
            data = self.get_player_data(player_uuid)
            
            # Clean up state
            data.state = GameState.LOBBY
            data.rollback_buffer.clear()
            data.current_match = None
            
            # 1. Clear Inventory & Effects
            player.inventory.clear()
            self.server.dispatch_command(self.server.command_sender, f'effect "{player.name}" clear')
            
            # 2. Teleport to Lobby
            lobby = self.plugin_config.get("lobby_spawn", [0.5, 100.0, 0.5])
            config_dim = self.plugin_config.get("world_name", "minecraft:overworld")
            dimension = self.get_safe_dimension(config_dim)
            
            if dimension:
                loc = Location(dimension, float(lobby[0]), float(lobby[1]), float(lobby[2]))
                player.teleport(loc)
                player.game_mode = GameMode.SURVIVAL
            
            # 3. Give Compass
            try:
                # Attempt to give locked item via NBT command
                self.server.dispatch_command(
                    self.server.command_sender,
                    f'give "{player.name}" lodestone_compass 1 0 {{"minecraft:item_lock":{{"mode":"lock_in_inventory"}}}}'
                )
            except:
                player.inventory.set_item(0, ItemStack("minecraft:lodestone_compass", 1))
                
            player.send_message(f"{ColorFormat.GOLD}Welcome to the Lobby! Use the compass to play.{ColorFormat.RESET}")

        except Exception as e:
            self.logger.error(f"Error resetting player: {e}")

    # --- Interaction & Menus ---

    @event_handler
    def on_player_interact(self, event: PlayerInteractEvent) -> None:
        player = event.player
        item = player.inventory.item_in_main_hand
        
        if item and item.type == "minecraft:lodestone_compass":
            data = self.get_player_data(str(player.unique_id))
            if data.state == GameState.LOBBY:
                self.open_category_menu(player)
    
    @event_handler
    def on_player_drop_item(self, event: PlayerDropItemEvent) -> None:
        if event.item_drop.item_stack.type == "minecraft:lodestone_compass":
            event.cancelled = True

    def open_category_menu(self, player) -> None:
        form = ActionForm()
        form.title = "Select Mode"
        for category in self.plugin_config.get("categories", {}):
            form.add_button(category, on_click=lambda p, c=category: self.open_match_menu(p, c))
        player.send_form(form)

    def open_match_menu(self, player, category: str) -> None:
        form = ActionForm()
        form.title = f"{category} Arenas"
        matches = self.plugin_config.get("categories", {}).get(category, {})
        
        for match_name, match_data in matches.items():
            # Calculate players in this specific match
            count = sum(1 for p in self.player_data.values() 
                       if p.state == GameState.MATCH and p.current_match == match_name)
            max_p = match_data.get("max_players", 2)
            
            form.add_button(f"{match_name} [{count}/{max_p}]", 
                          on_click=lambda p, c=category, m=match_name: self.handle_match_select(p, c, m))
        player.send_form(form)

    def handle_match_select(self, player, category, match_name) -> None:
        data = self.get_player_data(str(player.unique_id))
        match_data = self.plugin_config["categories"][category][match_name]
        
        # Teleport Logic
        spawn = match_data.get("spawn")
        dim_name = self.plugin_config.get("world_name", "minecraft:overworld")
        dimension = self.get_safe_dimension(dim_name)
        
        if dimension:
            data.state = GameState.MATCH
            data.current_category = category
            data.current_match = match_name
            data.world_name = dim_name
            
            # Setup Rollback File (JSONL)
            data.json_path = self.rollback_dir / f"rollback_{data.uuid}.jsonl"
            # Clear old file if exists
            if data.json_path.exists():
                data.json_path.unlink()
            
            player.inventory.clear()
            player.teleport(Location(dimension, float(spawn[0]), float(spawn[1]), float(spawn[2])))
            player.send_message(f"{ColorFormat.GREEN}Match Started: {match_name}!")
            player.send_message(f"{ColorFormat.GRAY}Mob griefing ignored. Blocks will rollback.")

    # --- Recording System (JSONL) ---

    @event_handler
    def on_block_break(self, event: BlockBreakEvent) -> None:
        self.record_action(event.player, "break", event.block)

    @event_handler
    def on_block_place(self, event: BlockPlaceEvent) -> None:
        self.record_action(event.player, "place", event.block)

    def record_action(self, player, action_type: str, block) -> None:
        data = self.get_player_data(str(player.unique_id))
        if data.state != GameState.MATCH:
            return

        # Create dictionary entry
        entry = {
            "t": round(datetime.now().timestamp(), 2),
            "a": action_type,
            "x": block.x,
            "y": block.y,
            "z": block.z,
            "b": block.type # "minecraft:stone"
        }
        
        data.rollback_buffer.append(entry)

    def flush_all_buffers(self) -> None:
        """Writes memory buffers to JSONL files"""
        for data in self.player_data.values():
            if data.state == GameState.MATCH and data.rollback_buffer and data.json_path:
                try:
                    with open(data.json_path, 'a') as f:
                        for entry in data.rollback_buffer:
                            f.write(json.dumps(entry) + "\n")
                    data.rollback_buffer.clear()
                except Exception as e:
                    self.logger.error(f"Flush error: {e}")

    # --- Death & Respawn (The Fix) ---

    @event_handler
    def on_player_death(self, event: PlayerDeathEvent) -> None:
        player = event.player
        uuid = str(player.unique_id)
        data = self.get_player_data(uuid)
        
        if data.state == GameState.MATCH:
            # 1. Flush remaining actions
            self.flush_all_buffers()
            
            # 2. Trigger Rollback immediately
            self.start_rollback(uuid)
            
            # Lightning effect
            try:
                self.server.dispatch_command(self.server.command_sender, 
                                           f'summon lightning_bolt {player.location.x} {player.location.y} {player.location.z}')
            except: pass

    @event_handler
    def on_player_respawn(self, event: PlayerRespawnEvent) -> None:
        """
        CRITICAL FIX: If the map is still rolling back, do NOT let them play.
        Put them in spectator mode until the 'finish_rollback' function releases them.
        """
        player = event.player
        uuid = str(player.unique_id)
        data = self.get_player_data(uuid)

        if data.state == GameState.ROLLBACK:
            player.send_message(f"{ColorFormat.RED}Map resetting... please wait.")
            player.game_mode = GameMode.SPECTATOR
            # We don't teleport them yet; finish_rollback will handle the lobby teleport
        else:
            self.handle_respawn_normal(player)

    def handle_respawn_normal(self, player):
        """Standard lobby respawn"""
        self.reset_player(player)

    @event_handler
    def on_player_quit(self, event: PlayerQuitEvent) -> None:
        uuid = str(event.player.unique_id)
        data = self.get_player_data(uuid)
        if data.state == GameState.MATCH:
            self.flush_all_buffers()
            self.start_rollback(uuid)
            
    # --- Rollback System (High Performance) ---

    def start_rollback(self, uuid: str) -> None:
        data = self.get_player_data(uuid)
        
        # Avoid double starts
        if data.state == GameState.ROLLBACK and uuid in self.rollback_tasks:
            return

        data.state = GameState.ROLLBACK
        self.logger.info(f"Starting rollback for {uuid}")

        # Read JSONL file into memory
        actions = []
        if data.json_path and data.json_path.exists():
            try:
                with open(data.json_path, 'r') as f:
                    for line in f:
                        if line.strip():
                            actions.append(json.loads(line))
                actions.reverse() # Undo newest first
            except Exception as e:
                self.logger.error(f"Read error: {e}")

        data.pending_rollback_actions = actions
        
        # Schedule Fast Task (1 tick period = fastest possible)
        task = self.server.scheduler.run_task(
            self, 
            lambda: self.process_rollback_batch(uuid), 
            delay=1, 
            period=1 
        )
        self.rollback_tasks[uuid] = task.task_id

    def process_rollback_batch(self, uuid: str) -> None:
        """Processes a large batch of blocks per tick"""
        data = self.get_player_data(uuid)
        actions = data.pending_rollback_actions
        
        if not actions:
            self.finish_rollback(uuid)
            return

        # BATCH SIZE: Process 100 blocks per tick (2000 blocks/sec)
        # This makes rollback nearly instant
        BATCH_SIZE = 100 
        
        count = 0
        while count < BATCH_SIZE and actions:
            action = actions.pop(0)
            self.revert_single_action(action, data.world_name)
            count += 1
            
        if not actions:
            self.finish_rollback(uuid)

    def revert_single_action(self, action: Dict, world_name: str) -> None:
        try:
            # Safe dimension retrieval
            dim = self.get_safe_dimension(world_name or "minecraft:overworld")
            if not dim: return

            x, y, z = int(action['x']), int(action['y']), int(action['z'])
            block = dim.get_block_at(x, y, z)
            
            # Logic: If they placed it, turn to Air. If they broke it, turn back to original type.
            if action['a'] == 'place':
                block.type = "minecraft:air"
            elif action['a'] == 'break':
                block.type = action['b'] # Restores type (e.g. "minecraft:cobblestone")
                
        except Exception as e:
            # Log error but don't crash loop
            pass

    def finish_rollback(self, uuid: str) -> None:
        data = self.get_player_data(uuid)
        
        # Cancel task
        if uuid in self.rollback_tasks:
            try: self.server.scheduler.cancel_task(self.rollback_tasks[uuid])
            except: pass
            del self.rollback_tasks[uuid]

        # Delete file
        if data.json_path and data.json_path.exists():
            try: data.json_path.unlink()
            except: pass

        # Find player and reset
        try:
            player = self.server.get_player(uuid_module.UUID(uuid))
            if player:
                # This releases them from Spectator Mode -> Lobby
                self.reset_player(player)
            else:
                data.state = GameState.LOBBY
        except:
            data.state = GameState.LOBBY

    # --- Resuming Crashed Rollbacks ---
    
    def resume_incomplete_rollbacks(self) -> None:
        """Finds orphan .jsonl files from server crash"""
        if not self.rollback_dir.exists(): return
        
        for file_path in self.rollback_dir.glob("rollback_*.jsonl"):
            try:
                uuid_str = file_path.stem.replace("rollback_", "")
                self.logger.info(f"Resuming crash rollback for {uuid_str}")
                
                data = self.get_player_data(uuid_str)
                data.json_path = file_path
                data.world_name = self.plugin_config.get("world_name")
                self.start_rollback(uuid_str)
            except:
                pass

    def on_command(self, sender: CommandSender, command: Command, args: list[str]) -> bool:
        if command.name == "out" and hasattr(sender, "unique_id"):
            uuid = str(sender.unique_id)
            if self.get_player_data(uuid).state == GameState.MATCH:
                self.flush_all_buffers()
                self.start_rollback(uuid)
                sender.send_message("Exiting match...")
                return True
        return False
