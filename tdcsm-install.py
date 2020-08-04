#! /usr/bin/env python
import sys

def check_python():
	"Check if the correct python version is installed"
	if sys.version_info < (3, 0):
		raise SystemExit("Python2 is not supported. Try rerunning with python3 or download the latest **64-bit** version from https://www.python.org/downloads/")
	if sys.maxsize <= 2**32:
		raise SystemExit("Only Python **64-bit** version is supported. Please uninstall and install the latest Python3 64-bit version")
	if sys.version_info < (3, 8, 5):
		raise SystemExit("Incorrect Python version %d.%d.%d. Must use a version >= 3.8.5" % (sys.version_info[:3]))

	try:
		import tkinter
		tcl_tk_installed = True
	except ImportError:
		tcl_tk_installed = False

	if not tcl_tk_installed:
		raise SystemExit("Python isn't installed with Tcl/Tk support enabled")

def venv_base():
	from pathlib import Path
	return Path.home() / ".py" / "tdcsm"

def venv_bin(exec="python"):
	from platform import system
	return venv_base() / "Scripts" / f"{exec}.exe" if system() == "Windows" else venv_base() / "bin" / exec

def run(*args):
	import subprocess

	print("Running: '{}'...".format(" ".join(args)), flush=True, end='')
	subprocess.run(args)
	print("done")

def create_venv():
	from logging import getLogger

	if venv_bin().exists():
		getLogger().warning(f"Found '{venv_bin()}', skipping recreating virtual environment")
		return

	venv_base().mkdir(parents=True, exist_ok=True)

	run(sys.executable, "-m", "venv", str(venv_base()))
	run(str(venv_bin()), "-m", "pip", "install", "--upgrade", "pip", "wheel")

def install_tdcsm():
	run(str(venv_bin()), "-m", "pip", "install", "--upgrade", "tdcsm")

def create_shortcut():
	from platform import system
	from pathlib import Path

	print("Creating a desktop shortcut...", end='')

	if system() == "Windows":
		with open(Path.home() / "Desktop" / "tdcsm-cmd.bat", "w") as fh:
			fh.write(f"%ComSpec% /K " + str(venv_base() / "Scripts" / "activate.bat") + "\n")
	else:
		with open(Path.home() / "Desktop" / "tdcsm-cmd.sh", "w") as fh:
			fh.write(f"#! /bin/sh\nbash --rcfile <(echo '. ~/.bashrc;. ~/.py/tdcsm/bin/activate')\n")

	print("done", end='')

def main():
	check_python()
	create_venv()
	install_tdcsm()
	create_shortcut()

if __name__ == "__main__":
	main()
