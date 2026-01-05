# 🎨 Semantic Sync Dashboard - Color Palette Documentation

## Overview

This document defines the approved color palette for the Semantic Sync Dashboard UI. 
**All colors must be sourced from `styles.py`** - never use hardcoded color values.

---

## 🎯 Design Token System

All colors are centralized in `styles.py` in the `COLORS` dictionary. This ensures:

1. **Consistency** - All components use the same palette
2. **Maintainability** - Change colors in one place
3. **Accessibility** - Easier to audit contrast ratios
4. **Theme Support** - Easy to add light/dark mode switching

---

## 🎨 Color Palette

### Primary Brand Colors
| Token | Hex | Usage |
|-------|-----|-------|
| `primary` | `#667eea` | Main brand color, CTAs, links, active states |
| `primary_hover` | `#5a6fd6` | Hover state for primary |
| `primary_light` | `#8b9ff5` | Light variant for highlights |
| `primary_dark` | `#4c5dc2` | Dark variant for pressed states |

### Secondary Brand Colors
| Token | Hex | Usage |
|-------|-----|-------|
| `secondary` | `#764ba2` | Secondary actions, gradients |
| `secondary_hover` | `#6a4392` | Hover state |
| `secondary_light` | `#9067b5` | Light variant |
| `secondary_dark` | `#5c3a82` | Dark variant |

### Accent Colors (Snowflake Theme)
| Token | Hex | Usage |
|-------|-----|-------|
| `accent_snowflake` | `#29b5e8` | Snowflake-related UI elements |
| `accent_snowflake_light` | `#4fc7f0` | Light variant |
| `accent_snowflake_dark` | `#1a9dc8` | Dark variant |

### Semantic Status Colors
| Token | Hex | Usage |
|-------|-----|-------|
| `success` | `#10b981` | Connected states, successful operations |
| `success_bg` | `rgba(16, 185, 129, 0.12)` | Success card backgrounds |
| `warning` | `#f59e0b` | Warnings, pending states |
| `warning_bg` | `rgba(245, 158, 11, 0.12)` | Warning backgrounds |
| `error` | `#ef4444` | Errors, disconnected states |
| `error_bg` | `rgba(239, 68, 68, 0.12)` | Error backgrounds |
| `info` | `#3b82f6` | Informational messages |
| `info_bg` | `rgba(59, 130, 246, 0.12)` | Info backgrounds |

### Text Colors
| Token | Hex | Usage |
|-------|-----|-------|
| `text_primary` | `#1e293b` | Main headings (light mode only - **AVOID**) |
| `text_secondary` | `#475569` | Body text |
| `text_muted` | `#64748b` | Secondary/helper text |
| `text_light` | `#94a3b8` | Very light text |
| `text_on_primary` | `#f8fafc` | Text on colored backgrounds |
| `text_on_dark` | `#f1f5f9` | Text on dark backgrounds |

### Background Colors
| Token | Hex | Usage |
|-------|-----|-------|
| `bg_app` | `#0f0f23` | Main app background |
| `bg_card` | `#1a1a2e` | Card/container backgrounds |
| `bg_card_hover` | `#252542` | Card hover state |
| `bg_input` | `#252542` | Form element backgrounds |
| `bg_elevated` | `#222240` | Elevated surfaces (modals, dropdowns) |

### Border Colors
| Token | Hex | Usage |
|-------|-----|-------|
| `border_default` | `rgba(102, 126, 234, 0.2)` | Default borders |
| `border_light` | `rgba(255, 255, 255, 0.08)` | Light decorative borders |
| `border_focus` | `#667eea` | Focus/active borders |
| `border_card` | `rgba(102, 126, 234, 0.15)` | Card borders |

---

## ⚠️ FORBIDDEN COLORS

The following colors are **BANNED** from use anywhere in the codebase:

| Forbidden | Reason |
|-----------|--------|
| `#FFFFFF` | Pure white breaks dark theme |
| `white` | Pure white breaks dark theme |
| `rgb(255, 255, 255)` | Pure white breaks dark theme |
| `#fff` | Pure white breaks dark theme |
| `#f8fafc` (as background) | Too light for dark theme |

**If you need a "light" element**, use:
- `bg_card` for card backgrounds
- `bg_elevated` for elevated surfaces
- `text_on_dark` for light text

---

## 🛠️ Usage Examples

### In Python (app.py)
```python
from styles import COLORS, get_status_color, get_card_style

# ✅ CORRECT - Use COLORS dictionary
status_color = COLORS['success']
bg_color = COLORS['bg_card']

# ✅ CORRECT - Use helper functions
status = get_status_color(is_connected)
card_css = get_card_style('fabric')

# ❌ WRONG - Hardcoded colors
status_color = "#10b981"
bg_color = "white"
```

### In Inline Styles (HTML/CSS in markdown)
```python
# ✅ CORRECT
st.markdown(f"""
<div style="background: {COLORS['bg_card']}; color: {COLORS['text_on_dark']};">
    Content here
</div>
""", unsafe_allow_html=True)

# ❌ WRONG
st.markdown("""
<div style="background: white; color: #1e293b;">
    Content here
</div>
""", unsafe_allow_html=True)
```

---

## 🔍 Auditing for Violations

Run this command to find hardcoded colors:
```bash
# Find white values
grep -r "white\|#fff\|#ffffff\|rgb(255" frontend/ --include="*.py"

# Find any hardcoded hex colors
grep -rE "#[0-9a-fA-F]{6}|#[0-9a-fA-F]{3}" frontend/app.py
```

---

## 📋 Checklist Before Committing

- [ ] No hardcoded color values in app.py
- [ ] All new components use `COLORS` dictionary
- [ ] New status indicators use `get_status_color()`
- [ ] New cards use `get_card_style()` variant
- [ ] Tested in dark mode browser
- [ ] Checked accessibility contrast ratios

---

## 🔄 Adding New Colors

1. Add to `COLORS` dictionary in `styles.py`
2. Add corresponding CSS variable in `get_css_variables()`
3. Update this documentation
4. Create helper function if commonly used

---

*Last Updated: January 2026*
*Maintained by: Semantic Sync Dev Team*
