# FortCore

High-performance PvP Core plugin for Endstone Bedrock servers with advanced rollback system.

## Features

? **Zero-Lag Performance**
- Async rollback processing
- RAM buffering with 60-second disk flush
- Staggered action processing (2 actions per 0.5s)
- No blocking operations on main thread

?? **Game State Management**
- Six states: LOBBY, QUEUE, TELEPORTING, MATCH, ROLLBACK, END
- Automatic state transitions
- Thread-safe player data handling

?? **Advanced Rollback System**
- Records all block breaks/placements
- CSV-based persistence per player UUID
- Automatic rollback on death/disconnect//out
- Slow, steady reversion prevents lag spikes

??? **Map & Kit System**
- Configurable maps with spawn points
- Configurable kits with player limits
- Auto-matching (Button 1 ? Map 1 + Kit 1)
- Real-time player count display

## Installation

### 1. Install via pip (Recommended)

```bash
pip install endstone-fortcore
```

### 2. Build from source

```bash
git clone <repository-url>
cd fortcore
pip install build
python -m build --wheel
pip install dist/endstone_fortcore-*.whl
```

### 3. Development Mode

```bash
pip install -e .
```

## Quick Start

1. **Install the plugin** using one of the methods above

2. **Start your Endstone server** - FortCore will create a default `config.yml`

3. **Configure your maps and kits** in `plugins/FortCore/config.yml`

4. **Restart the server** to apply changes

5. **Join the server** and right-click the Lodestone Compass to select a match!

## Configuration

### config.yml

```yaml
# Lobby spawn point (where players spawn on join/reset)
lobby_spawn:
  x: 0
  y: 100
  z: 0
  world: "world"

# Maps configuration
# IMPORTANT: Maps are matched by index with kits
# Button 1 ? Map 1 + Kit 1
maps:
  - name: "Diamond Arena"
    creator: "Admin"
    spawn:
      x: 100
      y: 64
      z: 100
    world: "world"
  
  - name: "Desert Battle"
    creator: "Builder123"
    spawn:
      x: 500
      y: 70
      z: -200
    world: "world"

# Kits configuration
# IMPORTANT: Kits must match maps by index
kits:
  - name: "Diamond SMP"
    creator: "Admin"
    maxPlayers: 8
  
  - name: "Knight Fight"
    creator: "PvPMaster"
    maxPlayers: 4
```

### Map & Kit Matching

**Critical Rule:** Button index must match both map and kit index.

Example:
- Button 1 displays: `Diamond SMP [4/8]`
- Clicking Button 1 teleports to: `Map[0]` with `Kit[0]`
- Button 2 displays: `Knight Fight [2/4]`
- Clicking Button 2 teleports to: `Map[1]` with `Kit[1]`

## Usage

### For Players

1. **Join the server** - You'll spawn in the lobby with a Lodestone Compass
2. **Right-click the compass** - Opens the FortCore match selection menu
3. **Select a match** - Choose from available kits (shows current players)
4. **Play the match** - All blocks you break/place are recorded
5. **Leave the match** - Type `/out` or die/disconnect to trigger rollback

### Commands

| Command | Description | Permission |
|---------|-------------|------------|
| `/out` | Leave current match and return to lobby | `fortcore.command.out` (default: true) |

### Menu Item

- **Item:** Lodestone Compass
- **Slot:** 9 (hotbar)
- **Features:**
  - Locked (cannot drop)
  - Persists on death
  - Opens match selection on right/left click

## How It Works

### Player Join Flow

```
Player Joins ? RESET ? Give Compass ? Apply Weakness ? Set LOBBY State
```

**RESET includes:**
- Gamemode: Survival
- Clear all potion effects
- Clear inventory (main + armor + offhand)
- Teleport to lobby spawn
- Give Lodestone Compass in slot 9
- Apply Weakness 255 (infinite, no particles)

### Match Join Flow

```
Click Kit ? Check State ? Check Capacity ? Set TELEPORTING
  ? 5s Global Cooldown ? Teleport ? Set MATCH ? Init Rollback
```

**On Match Join:**
- Shows map name & creator
- Shows kit name & creator
- Creates rollback CSV file
- Starts recording actions

### Rollback Flow

```
Trigger (Death/DC//out) ? Flush Buffer ? Read CSV (Reverse)
  ? Process 2 Actions/0.5s ? Revert Blocks ? Delete CSV ? RESET
```

**Rollback Triggers:**
- Player dies (with lightning effect)
- Player disconnects
- Player uses `/out` command

**Rollback Speed:**
- Every 0.5 seconds (10 ticks)
- 2 actions per cycle
- Prevents client/server lag

### Recording System

**RAM Buffer:**
- All actions stored in memory first
- Lightweight, fast operations

**Disk Flush:**
- Every 60 seconds (1200 ticks)
- Automatic scheduled task
- Prevents data loss

**Action Types:**
- `break` - Block broken by player
- `place` - Block placed by player

## Performance Features

### Zero-Lag Design

1. **Async Operations**
   - Rollback runs on scheduled tasks
   - Never blocks main thread
   - Staggered processing

2. **RAM Buffering**
   - Actions stored in memory
   - Periodic flush to disk
   - Minimal I/O operations

3. **Global Cooldown**
   - 5-second teleport cooldown per kit
   - Prevents collision
   - Smooth player flow

4. **Smart State Machine**
   - Six distinct states
   - Prevents race conditions
   - Thread-safe transitions

### File Structure

```
plugins/
+-- FortCore/
    +-- config.yml
    +-- rollbacks/
        +-- rollback_<uuid>.csv
        +-- rollback_<uuid>.csv
        +-- ...
```

**Rollback CSV Format:**
```csv
timestamp,action,x,y,z,block_type
1705420800.123,place,100,64,100,minecraft:stone
1705420801.456,break,100,64,101,minecraft:dirt
```

## API for Developers

### Events (Internal)

- `PlayerJoinEvent` - Reset player, give compass
- `PlayerDeathEvent` - Trigger rollback (with lightning)
- `PlayerQuitEvent` - Flush buffer, trigger rollback
- `BlockBreakEvent` - Record action to buffer
- `BlockPlaceEvent` - Record action to buffer

### Player States

```python
from endstone_fortcore.fortcore import GameState

# Check player state
data = plugin.get_player_data(player_uuid)
if data.state == GameState.MATCH:
    # Player is in a match
    pass
```

## Troubleshooting

### Issue: Players can't join matches

**Solution:** Check `config.yml`:
- Ensure maps and kits have same count
- Verify world names exist
- Check maxPlayers limits

### Issue: Rollback not working

**Solution:**
- Check `plugins/FortCore/rollbacks/` directory exists
- Verify CSV files are being created
- Check server logs for errors

### Issue: Menu not opening

**Solution:**
- Verify player has Lodestone Compass in slot 9
- Check if player is in LOBBY state
- Restart server to reload plugin

### Issue: Lag during rollback

**Solution:**
- This shouldn't happen with our staggered system
- Check if too many players rolling back simultaneously
- Verify rollback speed (default: 2 actions per 0.5s)

## Advanced Configuration

### Customize Rollback Speed

Edit `fortcore.py` line ~380:

```python
# Default: 2 actions every 0.5 seconds (10 ticks)
period=10  # Change to 5 for 4 actions/0.5s (faster but riskier)
```

### Customize Flush Interval

Edit `fortcore.py` line ~65:

```python
# Default: Every 60 seconds (1200 ticks)
period=1200  # Change to 2400 for 120 seconds
```

### Add Custom Effects on Match Join

Edit `teleport_to_match()` method to add potion effects, items, etc.

## Requirements

- Python 3.9+
- Endstone 0.5.0+
- PyYAML 6.0+
- Bedrock Dedicated Server

## Support

For issues, feature requests, or contributions:
- GitHub: [Your Repository URL]
- Discord: [Your Discord Server]

## License

[Your License Here]

## Credits

Developed for high-performance Bedrock PvP servers using Endstone.

---

**Note:** This plugin is optimized for performance. All operations are designed to minimize lag and provide smooth gameplay even with many simultaneous players and extensive block modifications.