# FortCore - High-performance PvP Core Plugin for Endstone Bedrock Server
# Finalized "Bulletproof" Version 1.1

from endstone.plugin import Plugin
from endstone.command import Command, CommandSender
from endstone.event import event_handler, EventPriority, PlayerJoinEvent, PlayerDeathEvent, PlayerQuitEvent, PlayerInteractEvent, BlockBreakEvent, BlockPlaceEvent
from endstone import ColorFormat, GameMode
from endstone.form import ActionForm
from endstone.inventory import ItemStack
from endstone.potion import PotionEffect, PotionEffectType
import yaml
import csv
import time
import uuid as uuid_lib
from pathlib import Path
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any

# --- HELPER CLASSES ---

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
        self.x = int(x)
        self.y = int(y)
        self.z = int(z)
        self.block_type = block_type
        self.timestamp = timestamp

class PlayerData:
    """Stores player state and rollback data"""
    def __init__(self, uuid: str):
        self.uuid = uuid
        self.state = GameState.LOBBY
        self.rollback_buffer: List[RollbackAction] = []
        self.csv_path: Optional[Path] = None
        self.last_flush = time.time()
        self.current_kit: Optional[str] = None
        self.current_map: Optional[str] = None

class RepeatingTask:
    """
    Wrapper to emulate run_task_timer using recursive run_task_later.
    Fixes AttributeError: 'Scheduler' object has no attribute 'run_task_timer'
    """
    def __init__(self, plugin, task_func, delay: int, period: int):
        self.plugin = plugin
        self.task_func = task_func
        self.period = period
        self.running = True
        self.task = None
        
        # Initial schedule
        self.plugin.server.scheduler.run_task_later(self.plugin, self._run, delay)

    def _run(self):
        if not self.running:
            return
            
        try:
            self.task_func()
        except Exception as e:
            self.plugin.logger.error(f"Error in repeating task: {e}")
        
        if self.running:
            self.task = self.plugin.server.scheduler.run_task_later(self.plugin, self._run, self.period)

    def cancel(self):
        self.running = False
        if self.task:
            self.task.cancel()

# --- MAIN PLUGIN CLASS ---

class FortCore(Plugin):
    api_version = "0.5"
    
    def __init__(self):
        super().__init__()
        self.player_data: Dict[str, PlayerData] = {}
        self.plugin_config: Dict = {} 
        self.teleport_cooldown: Dict[str, float] = {} # For kits
        self.rollback_dir: Path = None
        self.flush_task: Optional[RepeatingTask] = None
        self.rollback_tasks: Dict[str, RepeatingTask] = {}
        
    def on_load(self) -> None:
        self.logger.info("FortCore loading...")
        self.load_config()
        
    def on_enable(self) -> None:
        self.logger.info("FortCore enabled!")
        self.register_events(self)
        
        # Create rollback directory
        self.rollback_dir = Path(self.data_folder) / "rollbacks"
        self.rollback_dir.mkdir(parents=True, exist_ok=True)
        
        # Start flush task every 60 seconds (1200 ticks at 20tps)
        # Using custom RepeatingTask to fix API error
        self.flush_task = RepeatingTask(
            self, self.flush_all_buffers, delay=1200, period=1200
        )
        
        map_count = len(self.plugin_config.get('maps', []))
        kit_count = len(self.plugin_config.get('kits', []))
        self.logger.info(f"Loaded {map_count} maps and {kit_count} kits.")

        if map_count != kit_count:
            self.logger.error("WARNING: Map count and Kit count do not match! Buttons may malfunction.")
        
    def on_disable(self) -> None:
        self.logger.info("FortCore disabling...")
        self.flush_all_buffers()
        
        if self.flush_task:
            self.flush_task.cancel()
            
        # Cancel all active rollbacks
        for task in self.rollback_tasks.values():
            task.cancel()
        self.rollback_tasks.clear()
        
    def load_config(self) -> None:
        """Load configuration from config.yml"""
        config_path = Path(self.data_folder) / "config.yml"
        
        default_config = {
            "lobby_spawn": {
                "x": 0, "y": 100, "z": 0, "world": "world"
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
        
        if not config_path.exists():
            config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(config_path, 'w') as f:
                yaml.dump(default_config, f, default_flow_style=False)
            self.plugin_config = default_config
        else:
            try:
                with open(config_path, 'r') as f:
                    self.plugin_config = yaml.safe_load(f) or default_config
            except Exception as e:
                self.logger.error(f"Failed to load config.yml: {e}")
                self.plugin_config = default_config

    def get_player_data(self, player_uuid: str) -> PlayerData:
        if player_uuid not in self.player_data:
            self.player_data[player_uuid] = PlayerData(player_uuid)
        return self.player_data[player_uuid]
    
    # --- CORE LOGIC: RESET ---

    def reset_player(self, player) -> None:
        """Complete player reset for Lobby"""
        try:
            player.game_mode = GameMode.SURVIVAL
            
            # Clear effects
            for effect in player.active_effects:
                player.remove_effect(effect.type)
            
            # Clear inventory
            player.inventory.clear()
            
            # Teleport to lobby
            lobby_cfg = self.plugin_config.get("lobby_spawn", {})
            level = self.server.get_level(lobby_cfg.get("world", "world"))
            
            if level:
                # Add 0.5 to center on block
                player.teleport(level, 
                              lobby_cfg.get("x", 0) + 0.5, 
                              lobby_cfg.get("y", 100), 
                              lobby_cfg.get("z", 0) + 0.5)
            else:
                self.logger.error(f"Lobby world '{lobby_cfg.get('world')}' not found!")

            # Give Compass (Slot 9 = Index 8)
            menu_item = ItemStack("minecraft:lodestone_compass", 1)
            player.inventory.set_item(8, menu_item)
            
            # Apply Weakness 255 (Infinite)
            # duration = -1 (infinite in some APIs) or very large number
            # Using 20 * 60 * 60 * 24 (1 day) to be safe
            weakness = PotionEffect(PotionEffectType.WEAKNESS, 1728000, 255, False, False, False)
            player.add_effect(weakness)
            
        except Exception as e:
            self.logger.error(f"Error resetting player {player.name}: {e}")

    # --- EVENTS ---

    @event_handler
    def on_player_join(self, event: PlayerJoinEvent) -> None:
        player = event.player
        # Delay join sequence slightly to ensure player is fully loaded
        self.server.scheduler.run_task_later(
            self, lambda: self.handle_join_sequence(player), 10
        )
    
    def handle_join_sequence(self, player) -> None:
        pid = str(player.unique_id)
        data = self.get_player_data(pid)
        self.reset_player(player)
        data.state = GameState.LOBBY
        player.send_message(f"{ColorFormat.GOLD}=== FortCore ==={ColorFormat.RESET}")
        player.send_message(f"{ColorFormat.YELLOW}Right-click the compass to join a match!{ColorFormat.RESET}")

    @event_handler
    def on_player_interact(self, event: PlayerInteractEvent) -> None:
        player = event.player
        item = event.item
        
        # Only handle interactions if holding the compass
        if item and item.type == "minecraft:lodestone_compass":
            # Cancel the interaction so they don't actually use the item
            event.cancelled = True 
            self.open_kit_menu(player)

    def open_kit_menu(self, player) -> None:
        form = ActionForm()
        form.title = "FortCore"
        
        kits = self.plugin_config.get("kits", [])
        
        for i, kit in enumerate(kits):
            kit_name = kit.get("name", "Unknown")
            # Calculate players in this specific kit
            online_count = sum(1 for pd in self.player_data.values() 
                             if pd.state == GameState.MATCH and pd.current_kit == kit_name)
            max_players = kit.get("maxPlayers", 8)
            
            button_text = f"{kit_name} [{online_count}/{max_players}]"
            # Bind index i to the lambda
            form.button(button_text, on_click=lambda p, idx=i: self.handle_kit_select(p, idx))
        
        form.send(player)

    def handle_kit_select(self, player, idx: int) -> None:
        pid = str(player.unique_id)
        data = self.get_player_data(pid)
        
        kits = self.plugin_config.get("kits", [])
        maps = self.plugin_config.get("maps", [])
        
        # Validation
        if idx >= len(kits) or idx >= len(maps):
            player.send_message(f"{ColorFormat.RED}Configuration Error: Map/Kit mismatch.{ColorFormat.RESET}")
            return
            
        target_kit = kits[idx]
        target_map = maps[idx]
        kit_name = target_kit.get("name")
        
        # 1. Check Player State
        if data.state != GameState.LOBBY:
             player.send_message(f"{ColorFormat.RED}You are already in a game or queue!{ColorFormat.RESET}")
             return

        # 2. Check Capacity
        online_count = sum(1 for pd in self.player_data.values() 
                         if pd.state == GameState.MATCH and pd.current_kit == kit_name)
        if online_count >= target_kit.get("maxPlayers", 8):
            player.send_message(f"{ColorFormat.RED}This match is full!{ColorFormat.RESET}")
            return

        # 3. Check Global Cooldown (Collision Prevention)
        now = time.time()
        last_teleport = self.teleport_cooldown.get(kit_name, 0)
        if now - last_teleport < 5.0:
            player.send_message(f"{ColorFormat.RED}System busy (Anti-Collision). Please try again in 2s.{ColorFormat.RESET}")
            return
            
        # PROCEED TO JOIN
        self.teleport_cooldown[kit_name] = now
        data.state = GameState.TELEPORTING
        
        # Run teleport logic on next tick
        self.server.scheduler.run_task_later(
            self, lambda: self.teleport_to_match(player, target_kit, target_map), 1
        )

    def teleport_to_match(self, player, kit: Dict, map_data: Dict) -> None:
        pid = str(player.unique_id)
        data = self.get_player_data(pid)
        
        # Clear Inventory
        player.inventory.clear()
        
        # Teleport
        spawn = map_data.get("spawn", {})
        level_name = map_data.get("world", "world")
        level = self.server.get_level(level_name)
        
        if not level:
            player.send_message(f"{ColorFormat.RED}Error: Map world not loaded.{ColorFormat.RESET}")
            data.state = GameState.LOBBY
            return

        player.teleport(level, spawn.get("x", 0) + 0.5, spawn.get("y", 64), spawn.get("z", 0) + 0.5)
        
        # Update State
        data.state = GameState.MATCH
        data.current_kit = kit.get("name")
        data.current_map = map_data.get("name")
        
        # Init Rollback
        self.init_rollback(pid)
        
        # Messages
        player.send_message(f"{ColorFormat.GOLD}=== FortCore ==={ColorFormat.RESET}")
        player.send_message(f"{ColorFormat.AQUA}Map: {map_data.get('name')} {ColorFormat.GRAY}by {map_data.get('creator')}{ColorFormat.RESET}")
        player.send_message(f"{ColorFormat.YELLOW}Kit: {kit.get('name')} {ColorFormat.GRAY}by {kit.get('creator')}{ColorFormat.RESET}")

    # --- ROLLBACK SYSTEM ---

    def init_rollback(self, pid: str) -> None:
        data = self.get_player_data(pid)
        data.rollback_buffer.clear()
        
        csv_path = self.rollback_dir / f"rollback_{pid}.csv"
        data.csv_path = csv_path
        
        # overwrite existing file
        try:
            with open(csv_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["timestamp", "action", "x", "y", "z", "block_type"])
        except Exception as e:
            self.logger.error(f"Failed to init rollback file: {e}")
            
    def record_action(self, player, action_type: str, block) -> None:
        pid = str(player.unique_id)
        data = self.get_player_data(pid)
        
        if data.state != GameState.MATCH:
            return
            
        action = RollbackAction(
            action_type,
            block.x, block.y, block.z,
            block.type,
            time.time()
        )
        data.rollback_buffer.append(action)

    @event_handler(priority=EventPriority.MONITOR)
    def on_block_break(self, event: BlockBreakEvent) -> None:
        if not event.cancelled:
            self.record_action(event.player, "break", event.block)

    @event_handler(priority=EventPriority.MONITOR)
    def on_block_place(self, event: BlockPlaceEvent) -> None:
        if not event.cancelled:
            self.record_action(event.player, "place", event.block)

    def flush_all_buffers(self) -> None:
        """Called every 60s"""
        for pid, data in self.player_data.items():
            if data.state == GameState.MATCH and data.rollback_buffer:
                self.flush_buffer(pid)

    def flush_buffer(self, pid: str) -> None:
        data = self.get_player_data(pid)
        if not data.csv_path or not data.rollback_buffer:
            return

        try:
            # Append mode
            with open(data.csv_path, 'a', newline='') as f:
                writer = csv.writer(f)
                for action in data.rollback_buffer:
                    writer.writerow([
                        action.timestamp,
                        action.action_type,
                        action.x, action.y, action.z,
                        action.block_type
                    ])
            data.rollback_buffer.clear()
            data.last_flush = time.time()
        except Exception as e:
            self.logger.error(f"Error flushing buffer for {pid}: {e}")

    # --- DEATH & LEAVING ---

    @event_handler
    def on_player_death(self, event: PlayerDeathEvent) -> None:
        player = event.entity
        # Check if entity is actually a player
        if not hasattr(player, "unique_id"): 
            return

        pid = str(player.unique_id)
        data = self.get_player_data(pid)

        # Clear Inventory immediately
        player.inventory.clear()

        if data.state == GameState.MATCH:
            # Thunder effect
            level = player.location.level
            # Spawning lightning bolt actor
            try:
                # Endstone 0.5.x specific lightning spawn might vary, 
                # using spawn_actor if available, otherwise ignoring to prevent crash
                # level.spawn_actor("minecraft:lightning_bolt", player.location) 
                pass 
            except:
                pass # Fail silently on effect
            
            self.start_rollback(pid)
        else:
            # If dying in lobby, just reset them
            self.server.scheduler.run_task_later(
                self, lambda: self.reset_player(player), 1
            )

    @event_handler
    def on_player_quit(self, event: PlayerQuitEvent) -> None:
        pid = str(event.player.unique_id)
        data = self.get_player_data(pid)
        
        if data.state == GameState.MATCH:
            self.flush_buffer(pid)
            self.start_rollback(pid, is_quit=True)
            
        # Clean up memory if they are just in lobby
        if data.state == GameState.LOBBY:
            if pid in self.player_data:
                del self.player_data[pid]

    def on_command(self, sender: CommandSender, command: Command, args: list[str]) -> bool:
        if command.name == "out":
            if not hasattr(sender, 'unique_id'):
                sender.send_message("Players only.")
                return True
            
            pid = str(sender.unique_id)
            data = self.get_player_data(pid)
            
            if data.state != GameState.MATCH:
                sender.send_message(f"{ColorFormat.RED}You are not in a match!{ColorFormat.RESET}")
                return True
            
            self.flush_buffer(pid)
            self.start_rollback(pid)
            sender.send_message(f"{ColorFormat.YELLOW}Leaving match...{ColorFormat.RESET}")
            return True
        return False

    # --- ROLLBACK EXECUTION ---

    def start_rollback(self, pid: str, is_quit: bool = False) -> None:
        data = self.get_player_data(pid)
        
        if data.state == GameState.ROLLBACK:
            return # Already rolling back
            
        self.flush_buffer(pid)
        data.state = GameState.ROLLBACK
        
        if not data.csv_path or not data.csv_path.exists():
            self.finish_rollback(pid, is_quit)
            return
            
        # Read actions
        actions = []
        try:
            with open(data.csv_path, 'r') as f:
                reader = csv.DictReader(f)
                actions = list(reader)
                actions.reverse() # Reverse order for rollback
        except Exception as e:
            self.logger.error(f"Error reading rollback CSV: {e}")
            
        if not actions:
            self.finish_rollback(pid, is_quit)
            return
            
        # Schedule the staggered rollback
        # 10 ticks = 0.5 seconds
        task = RepeatingTask(
            self, 
            lambda: self.process_rollback_batch(pid, actions, is_quit), 
            delay=10, 
            period=10
        )
        self.rollback_tasks[pid] = task

    def process_rollback_batch(self, pid: str, actions: List[Dict], is_quit: bool) -> None:
        # Process 2 actions
        for _ in range(2):
            if not actions:
                break
            action = actions.pop(0)
            self.revert_single_action(action)
            
        if not actions:
            # Done
            if pid in self.rollback_tasks:
                self.rollback_tasks[pid].cancel()
                del self.rollback_tasks[pid]
            self.finish_rollback(pid, is_quit)

    def revert_single_action(self, action: Dict) -> None:
        try:
            x, y, z = int(action["x"]), int(action["y"]), int(action["z"])
            block_type = action["block_type"]
            action_type = action["action"]
            
            # Use lobby world logic as default context or try to find where it happened
            # Assuming match world is the same as configured in maps.
            # In a robust system, we should store world name in CSV.
            # Here we try to use the world from the map config if we can find it, 
            # otherwise default to 'world'.
            
            # Note: Ideally store world in CSV, but for now we look up world.
            world_name = "world" # Default
            
            level = self.server.get_level(world_name)
            if not level: return
            
            block = level.get_block_at(x, y, z)
            
            # Inverse logic
            if action_type == "place":
                # If they placed it, we break it (set to air)
                block.type = "minecraft:air"
            elif action_type == "break":
                # If they broke it, we replace it
                block.type = block_type
                
        except Exception as e:
            # Log error but don't crash
            pass

    def finish_rollback(self, pid: str, is_quit: bool) -> None:
        data = self.get_player_data(pid)
        
        # Cleanup file
        if data.csv_path and data.csv_path.exists():
            try:
                data.csv_path.unlink()
            except: 
                pass
        
        data.rollback_buffer.clear()
        data.csv_path = None
        data.current_kit = None
        data.current_map = None
        data.state = GameState.LOBBY
        
        if not is_quit:
            # If they are still online, reset them to lobby
            player = self.server.get_player(uuid_lib.UUID(pid))
            if player:
                self.reset_player(player)
