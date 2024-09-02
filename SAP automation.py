import subprocess
import time

# Open Calculator app on macOS
subprocess.Popen(["open", "-a", "SAPGUI 7.50.app"])

# Give some time for the Calculator to open
time.sleep(2)

# AppleScript to perform the calculation (123 * 456) + (789 / 3)
calculation_script = """
tell application "System Events"
    tell process "Calculator"
        key code 18 -- 1
        key code 19 -- 2
        key code 20 -- 3
        key code 67 -- *
        key code 21 -- 4
        key code 22 -- 5
        key code 23 -- 6
        key code 36 -- return (enter key)
        key code 44 -- + (clears previous result)
        key code 18 -- 1
        key code 19 -- 2
        key code 20 -- 3
        key code 67 -- *
        key code 21 -- 4
        key code 22 -- 5
        key code 23 -- 6
        key code 36 -- return (enter key)
        key code 44 -- +
        key code 7 -- 7
        key code 8 -- 8
        key code 9 -- 9
        key code 75 -- /
        key code 20 -- 3
        key code 36 -- return (enter key)
        key code 36 -- return (enter key)
    end tell
end tell
"""

# Save the AppleScript to a temporary file and execute it
with open("calculation_script.scpt", "w") as script_file:
    script_file.write(calculation_script)

subprocess.Popen(["osascript", "calculation_script.scpt"])
