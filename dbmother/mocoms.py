# Author: Federico Tomassini aka efphe
# Thanks to: http://wubook.net/ http://en.wubook.net/
# License: BSD license. See `LICENSE` file at the root
#          of python module
DEFCOL = "\033[0m"
BLACKCOL = "\033[0;30m"
REDCOL = "\033[0;31m"
GREENCOL = "\033[0;32m"
BROWNCOL = "\033[0;33m"
BLUECOL = "\033[0;34m"
PURPLECOL = "\033[0;35m"
CYANCOL = "\033[0;36m"
LIGHTGRAYCOL = "\033[0;37m"
DARKGRAYCOL = "\033[1;30m"
LIGHTREDCOL = "\033[1;31m"
LIGHTGREENCOL = "\033[1;32m"
YELLOWCOL = "\033[1;33m"
LIGHTBLUECOL = "\033[1;34m"
MAGENTACOL = "\033[1;35m"
LIGHTCYANCOL = "\033[1;36m"
WHITECOL = "\033[1;37m"

def _STRCOLOR(s, col):
  return \
    "%s%s%s" %(col, s, DEFCOL) or str(s)
def RED(s):
  return _STRCOLOR(s, REDCOL)
def GREEN(s):
  return _STRCOLOR(s, GREENCOL)
def YELLOW(s):
  return _STRCOLOR(s, YELLOWCOL)
def PURPLE(s):
  return _STRCOLOR(s, PURPLECOL)

