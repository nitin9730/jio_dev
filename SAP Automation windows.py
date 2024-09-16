import time
import win32com.client
import subprocess

try:


    # Replace with the correct path to your SAP GUI executable
    sap_gui_path = r"C:\Program Files\SAP\NWBC770\NWBC.exe"

    # Start the SAP GUI application
    subprocess.Popen([sap_gui_path])
    print("SAP GUI opened successfully.")

    # Wait for the application to load
    time.sleep(10)

    # Connect to SAP GUI Scripting
    SapGuiAuto = win32com.client.GetObject("SAPGUISERVER")
    application = SapGuiAuto.GetScriptingEngine

    # Connect to the first active connection and session
    connection = application.Children(0)
    session = connection.Children(0)

    # Resize the working pane
    session.findById("wnd[0]").resizeWorkingPane(143, 16, False)
    
    time.sleep(1)  # Wait for the pane to resize

    # Send command to execute the transaction
    session.findById("wnd[0]").sendVKey(0)
    time.sleep(1)  # Wait for the command to process
    
    # Navigate the menu
    session.findById("wnd[0]/mbar/menu[3]/menu[0]").select()
    time.sleep(1)  # Wait for the menu selection to complete
    
    # Fill in the fields
    session.findById("wnd[0]/usr/ctxtS_BUKRS-LOW").text = "9008"
    session.findById("wnd[0]/usr/txtS_TTYPE-LOW").text = "5379"
    session.findById("wnd[0]/usr/ctxtS_PST_DT-LOW").text = "01.04.2023"
    session.findById("wnd[0]/usr/ctxtS_PST_DT-HIGH").text = "30.04.2023"
    session.findById("wnd[0]/usr/ctxtP_LVTEMP").text = "GL RECO KSB"
    
    # Set focus and caret position
    session.findById("wnd[0]/usr/ctxtP_LVTEMP").setFocus()
    session.findById("wnd[0]/usr/ctxtP_LVTEMP").caretPosition = 11
    
    time.sleep(1)  # Wait for focus change
    
    # Press buttons
    session.findById("wnd[0]/tbar[1]/btn[8]").press()
    time.sleep(1)  # Wait for processing
    
    session.findById("wnd[0]/usr/cntlGRID/shellcont/shell/shellcont[0]/shell/shellcont[1]/shell").pressButton("&XXL")
    time.sleep(1)  # Wait for the button press to process
    
    # Fill in save file details
    session.findById("wnd[1]/usr/ctxtDY_PATH").text = r"C:\Users\krishna.bathija\Downloads\GL Recon Python\460 data extract"
    session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = "Jiomoney Apr to June 23.XLSX"
    session.findById("wnd[1]/usr/ctxtDY_FILENAME").caretPosition = 28
    session.findById("wnd[1]/tbar[0]/btn[0]").press()
    time.sleep(1)  # Wait for file dialog to process
    
    # Set current cell and send function keys
    session.findById("wnd[0]/usr/cntlGRID/shellcont/shell/shellcont[1]/shell").setCurrentCell(4, "ERDAT")
    time.sleep(1)  # Wait for cell selection
    
    session.findById("wnd[0]").sendVKey(12)
    time.sleep(1)
    
    session.findById("wnd[0]/usr/ctxtS_BUKRS-LOW").caretPosition = 4
    session.findById("wnd[0]").sendVKey(12)
    session.findById("wnd[0]").sendVKey(12)

except Exception as e:
    print(f"An error occurred: {e}")
