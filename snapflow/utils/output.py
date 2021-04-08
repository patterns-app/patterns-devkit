from colorful import Colorful

success_symbol = " ✔ "
error_symbol = " ✖ "

cf = Colorful()
cf_palette = {
    "ghostWhite": "#F8F8F0",
    "lightGhostWhite": "#F8F8F2",
    "lightGray": "#CCCCCC",
    "gray": "#888888",
    "brownGray": "#49483E",
    "darkGray": "#282828",
    "yellow": "#E6DB74",
    "blue": "#66D9EF",
    "magenta": "#F92672",
    "purple": "#AE81FF",
    "brown": "#75715E",
    "orange": "#FD971F",
    "lightOrange": "#FFD569",
    "green": "#A6E22E",
    "seaGreen": "#529B2F",
}
cf_semantic_palette = {
    "error": "#D83925",
    "info": "#66D9EF",
    "warning": "#FD971F",
    "success": "#A6E22E",
}
colors = list(cf_palette)
cf_palette.update(cf_semantic_palette)
cf.update_palette(cf_palette)
