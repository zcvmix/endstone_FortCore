# FortCore - High-performance PvP Core Plugin for Endstone Bedrock Server
# Bulletproof Update 1.2

from endstone.plugin import Plugin
from endstone.command import Command, CommandSender
from endstone.event import event_handler, EventPriority, PlayerJoinEvent, PlayerDeathEvent, PlayerQuitEvent, PlayerInteractEvent, BlockBreakEvent, BlockPlaceEvent
from endstone import ColorFormat, GameMode
from endstone.form import ActionForm
from endstone.inventory import ItemStack
# FIXED IMPORT: Potion classes are often at the top level in Python API
from endstone import PotionEffect, PotionEffectType 
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
        self.teleport_cooldown: Dict[str, float] = {}
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
        
        # Start flush task (Every 60s = 1200 ticks)
        # Using custom RepeatingTask class
        self.flush_task = RepeatingTask(
            self, self.flush_all_buffers, delay=1200, period=1200
        )
        
        # Log loaded counts
        map_count = len(self.plugin_config.get('maps', []))
        kit_count = len(self.plugin_config.get('kits', []))
        self.logger.info(f"Loaded {map_count} maps and {kit_count} kits.")
        
    def on_disable(self) -> None:
        self.logger.info("FortCore disabling...")
        self.flush_all_buffers()
        
        if self.flush_task:
            self.flush_task.cancel()
            
        for task in self.rollback_tasks.values():
            task.cancel()
        self.rollback_tasks.clear()
        
    def load_config(self) -> None:
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
                self.logger.error(f"Failed to load config: {e}")
                self.plugin_config = default_config

    def get_player_data(self, player_uuid: str) -> PlayerData:
        if player_uuid not in self.player_data:
            self.player_data[player_uuid] = PlayerData(player_uuid)
        return self.player_data[player_uuid]
    
    # --- CORE: RESET ---
    
    def reset_player(self, player) -> None:
        """Reset player to lobby state"""
        try:
            player.game_mode = GameMode.SURVIVAL
            
            # Clear effects
            for effect in player.active_effects:
                player.remove_effect(effect.type)
            
            # Clear inventory
            player.inventory.clear()
            
            # Teleport
            lobby = self.plugin_config.get("lobby_spawn", {})
            level = self.server.get_level(lobby.get("world", "world"))
            if level:
                player.teleport(level, lobby.get("x", 0) + 0.5, lobby.get("y", 100), lobby.get("z", 0) + 0.5)
            
            # Give Compass
            menu_item = ItemStack("minecraft:lodestone_compass", 1)
            player.inventory.set_item(8, menu_item)
            
            # Apply Weakness (Infinite)
            # Duration: 20 ticks * 60 * 60 * 24 = 1728000 (1 day) to act as infinite
            weakness = PotionEffect(PotionEffectType.WEAKNESS, 1728000, 255, False, False, False)
            player.add_effect(weakness)
            
        except Exception as e:
            self.logger.error(f"Error resetting player: {e}")
            
    # --- EVENTS ---

    @event_handler
    def on_player_join(self, event: PlayerJoinEvent) -> None:
        player = event.player
        # Delay join handling slightly
        self.server.scheduler.run_task_later(
            self, lambda: self.handle_join_sequence(player), 10
        )
    
    def handle_join_sequence(self, player) -> None:
        pid = str(player.unique_id)
        data = self.get_player_data(pid)
        self.reset_player(player)
        data.state = GameState.LOBBY
        player.send_message(f"{ColorFormat.GOLD}=== FortCore ==={ColorFormat.RESET}")
        player.send_message(f"{ColorFormat.YELLOW}Right-click compass to play!{ColorFormat.RESET}")

    @event_handler
    def on_player_interact(self, event: PlayerInteractEvent) -> None:
        player = event.player
        item = event.item
        
        if item and item.type == "minecraft:lodestone_compass":
            event.cancelled = True
            self.open_kit_menu(player)

    def open_kit_menu(self, player) -> None:
        form = ActionForm()
        form.title = "FortCore"
        
        kits = self.plugin_config.get("kits", [])
        
        for i, kit in enumerate(kits):
            kit_name = kit.get("name", "Unknown")
            count = sum(1 for pd in self.player_data.values() 
                      if pd.state == GameState.MATCH and pd.current_kit == kit_name)
            max_p = kit.get("maxPlayers", 8)
            
            text = f"{kit_name} [{count}/{max_p}]"
            form.button(text, on_click=lambda p, idx=i: self.handle_kit_select(p, idx))
        
        form.send(player)

    def handle_kit_select(self, player, idx: int) -> None:
        pid = str(player.unique_id)
        data = self.get_player_data(pid)
        
        kits = self.plugin_config.get("kits", [])
        maps = self.plugin_config.get("maps", [])
        
        if idx >= len(kits) or idx >= len(maps):
            return
            
        target_kit = kits[idx]
        target_map = maps[idx]
        kit_name = target_kit.get("name")
        
        if data.state != GameState.LOBBY:
            player.send_message(f"{ColorFormat.RED}Already in game!{ColorFormat.RESET}")
            return
            
        # Capacity check
        count = sum(1 for pd in self.player_data.values() 
                  if pd.state == GameState.MATCH and pd.current_kit == kit_name)
        if count >= target_kit.get("maxPlayers", 8):
            player.send_message(f"{ColorFormat.RED}Full!{ColorFormat.RESET}")
            return
            
        # Cooldown
        now = time.time()
        last = self.teleport_cooldown.get(kit_name, 0)
        if now - last < 5.0:
            player.send_message(f"{ColorFormat.RED}Queue busy, please wait...{ColorFormat.RESET}")
            return
            
        self.teleport_cooldown[kit_name] = now
        data.state = GameState.TELEPORTING
        
        self.server.scheduler.run_task_later(
            self, lambda: self.teleport_to_match(player, target_kit, target_map), 1
        )

    def teleport_to_match(self, player, kit, map_data) -> None:
        pid = str(player.unique_id)
        data = self.get_player_data(pid)
        
        player.inventory.clear()
        
        spawn = map_data.get("spawn", {})
        level = self.server.get_level(map_data.get("world", "world"))
        if level:
            player.teleport(level, spawn.get("x", 0) + 0.5, spawn.get("y", 64), spawn.get("z", 0) + 0.5)
            
        data.state = GameState.MATCH
        data.current_kit = kit.get("name")
        data.current_map = map_data.get("name")
        
        self.init_rollback(pid)
        
        player.send_message(f"{ColorFormat.GOLD}=== MATCH STARTED ==={ColorFormat.RESET}")
        player.send_message(f"{ColorFormat.AQUA}Map: {map_data.get('name')}{ColorFormat.RESET}")

    # --- ROLLBACK ---

    def init_rollback(self, pid: str) -> None:
        data = self.get_player_data(pid)
        data.rollback_buffer.clear()
        
        csv_path = self.rollback_dir / f"rollback_{pid}.csv"
        data.csv_path = csv_path
        
        try:
            with open(csv_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["timestamp", "action", "x", "y", "z", "block_type"])
        except Exception as e:
            self.logger.error(f"Init rollback failed: {e}")

    def record_action(self, player, action_type: str, block) -> None:
        pid = str(player.unique_id)
        data = self.get_player_data(pid)
        
        if data.state == GameState.MATCH:
            action = RollbackAction(
                action_type, block.x, block.y, block.z, block.type, time.time()
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
        for pid, data in self.player_data.items():
            if data.state == GameState.MATCH and data.rollback_buffer:
                self.flush_buffer(pid)

    def flush_buffer(self, pid: str) -> None:
        data = self.get_player_data(pid)
        if not data.csv_path or not data.rollback_buffer:
            return
            
        try:
            with open(data.csv_path, 'a', newline='') as f:
                writer = csv.writer(f)
                for action in data.rollback_buffer:
                    writer.writerow([
                        action.timestamp, action.action_type,
                        action.x, action.y, action.z, action.block_type
                    ])
            data.rollback_buffer.clear()
            data.last_flush = time.time()
        except Exception as e:
            self.logger.error(f"Flush error: {e}")

    # --- DEATH & EXIT ---

    @event_handler
    def on_player_death(self, event: PlayerDeathEvent) -> None:
        player = event.entity
        if not hasattr(player, "unique_id"): return
        
        pid = str(player.unique_id)
        data = self.get_player_data(pid)
        player.inventory.clear()
        
        if data.state == GameState.MATCH:
            # Try thunder
            try:
                # level.spawn_actor("minecraft:lightning_bolt", player.location)
                pass 
            except: pass
            self.start_rollback(pid)
        else:
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
        elif data.state == GameState.LOBBY:
            if pid in self.player_data:
                del self.player_data[pid]

    def on_command(self, sender: CommandSender, command: Command, args: list[str]) -> bool:
        if command.name == "out" and hasattr(sender, 'unique_id'):
            pid = str(sender.unique_id)
            data = self.get_player_data(pid)
            if data.state == GameState.MATCH:
                self.flush_buffer(pid)
                self.start_rollback(pid)
                sender.send_message(f"{ColorFormat.YELLOW}Leaving match...{ColorFormat.RESET}")
                return True
        return False

    def start_rollback(self, pid: str, is_quit: bool = False) -> None:
        data = self.get_player_data(pid)
        if data.state == GameState.ROLLBACK: return
        
        self.flush_buffer(pid)
        data.state = GameState.ROLLBACK
        
        actions = []
        if data.csv_path and data.csv_path.exists():
            try:
                with open(data.csv_path, 'r') as f:
                    reader = csv.DictReader(f)
                    actions = list(reader)
                    actions.reverse()
            except: pass
            
        if not actions:
            self.finish_rollback(pid, is_quit)
            return
            
        task = RepeatingTask(
            self, lambda: self.process_rollback_batch(pid, actions, is_quit), 10, 10
        )
        self.rollback_tasks[pid] = task

    def process_rollback_batch(self, pid: str, actions: List[Dict], is_quit: bool) -> None:
        for _ in range(2):
            if not actions: break
            self.revert_action(actions.pop(0))
            
        if not actions:
            if pid in self.rollback_tasks:
                self.rollback_tasks[pid].cancel()
                del self.rollback_tasks[pid]
            self.finish_rollback(pid, is_quit)

    def revert_action(self, action: Dict) -> None:
        try:
            x, y, z = int(action["x"]), int(action["y"]), int(action["z"])
            action_type = action["action"]
            block_type = action["block_type"]
            
            # Simple assumption: world is "world"
            level = self.server.get_level("world")
            if not level: return
            
            block = level.get_block_at(x, y, z)
            
            if action_type == "place":
                block.type = "minecraft:air"
            elif action_type == "break":
                block.type = block_type
        except: pass

    def finish_rollback(self, pid: str, is_quit: bool) -> None:
        data = self.get_player_data(pid)
        if data.csv_path and data.csv_path.exists():
            try: data.csv_path.unlink()
            except: pass
            
        data.rollback_buffer.clear()
        data.csv_path = None
        data.state = GameState.LOBBY
        
        if not is_quit:
            player = self.server.get_player(uuid_lib.UUID(pid))
            if player: self.reset_player(player)
