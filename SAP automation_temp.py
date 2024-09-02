import os
import time
import pandas as pd


# Open Chrome with a specific URL
os.system("open -a 'Calculator' ")











Python Script - 

import time
import win32com.client as win32
import pandas as pd

def sap_connect():
    # Load Excel data
    excel_file = "C:\\Path\\To\\Your\\ZDECOMREP Status.xlsb"
    sheet_name = "Summary"
    
    excel_data = pd.read_excel(excel_file, sheet_name=sheet_name, engine='pyxlsb')
    
    login = excel_data.at[3, 'Q']
    password = excel_data.at[4, 'Q']
    
    connection_name = "P52_DIGITAL_ERP_SYSTEM"
    
    # Open SAP Logon
    shell = win32.Dispatch("WScript.Shell")
    shell.Run(r"C:\Program Files (x86)\SAP\FrontEnd\SAPgui\saplogon.exe")
    
    # Wait for SAP Logon window
    while not shell.AppActivate("SAP Logon"):
        time.sleep(1)
    
    # Connect to SAP
    sap_gui = win32.GetObject("SAPGUI")
    if not sap_gui:
        print("SAP GUI not found.")
        return
    
    app = sap_gui.GetScriptingEngine
    connection = app.OpenConnection(connection_name, True)
    session = connection.Children(0)
    
    # Login to SAP
    session.findById("wnd[0]").maximize()
    session.findById("wnd[0]/usr/txtRSYST-MANDT").Text = "452"
    session.findById("wnd[0]/usr/txtRSYST-BNAME").Text = login
    session.findById("wnd[0]/usr/pwdRSYST-BCODE").Text = password
    session.findById("wnd[0]/usr/txtRSYST-LANGU").Text = "EN"
    session.findById("wnd[0]/usr/txtRSYST-LANGU").SetFocus()
    session.findById("wnd[0]").sendVKey(0)
    
    # Handle multiple logon
    if session.Children.Count > 1:
        session.findById("wnd[1]/usr/radMULTI_LOGON_OPT2").Select()
        session.findById("wnd[1]/tbar[0]/btn[0]").press()
    
    # Execute transaction
    session.findById("wnd[0]/tbar[0]/okcd").Text = "Zdecomrep"
    session.findById("wnd[0]").sendVKey(0)
    
    # Interact with the SAP session
    session.findById("wnd[0]/tbar[1]/btn[17]").press()
    session.findById("wnd[1]/usr/txtENAME-LOW").Text = "p50115368"
    session.findById("wnd[1]/usr/txtENAME-LOW").SetFocus()
    session.findById("wnd[1]/usr/txtENAME-LOW").caretPosition = 9
    session.findById("wnd[1]/tbar[0]/btn[8]").press()
    session.findById("wnd[1]/usr/cntlALV_CONTAINER_1/shellcont/shell").selectedRows = "0"
    session.findById("wnd[1]/usr/cntlALV_CONTAINER_1/shellcont/shell").doubleClickCurrentCell()
    session.findById("wnd[0]/usr/txtS_VBELN-LOW").SetFocus()
    session.findById("wnd[0]/usr/btn%_S_VBELN_%_APP_%-VALU_PUSH").press()
    
    # Simulate Ctrl+V key press
    shell.SendKeys("^v")
    
    session.findById("wnd[1]/tbar[0]/btn[24]").press()
    session.findById("wnd[1]/tbar[0]/btn[8]").press()
    session.findById("wnd[0]/tbar[1]/btn[8]").press()
    session.findById("wnd[0]/mbar/menu[0]/menu[3]/menu[1]").Select()
    
    # Save file
    session.findById("wnd[1]/usr/ctxtDY_PATH").Text = "C:\\Users\\Priyabrata.T\\Downloads\\"
    session.findById("wnd[1]/usr/ctxtDY_FILENAME").Text = "ZDECOM.XLSX"
    session.findById("wnd[1]/tbar[0]/btn[11]").press()
    
    # Logout
    session.findById("wnd[0]/mbar/menu[5]/menu[12]").Select()
    session.findById("wnd[1]/usr/btnSPOP-OPTION1]").press()
    
    # Close Excel workbook
    excel_app = win32.Dispatch('Excel.Application')
    workbook = excel_app.Workbooks.Open("C:\\Users\\Priyabrata.T\\Downloads\\ZDECOM.XLSX")
    workbook.Close(False)
    excel_app.Quit()
    
if __name__ == "__main__":
    sap_connect()






VBA Script - 

Sub sapconnect()

    Dim wsinfo As Worksheet

    Dim connectionname As String

    Dim wshshell As Object

    Dim sapgui As Object

    Dim appl As Object

    Dim connection As Object

    Dim session As Object

   

    Dim login As String

    Dim pass As String

    Dim whno As String

    Dim i As Integer

    Dim lstrow As Long

   

    

        Set wsinfo = Workbooks("ZDECOMREP Status.xlsb").Worksheets("Summary")

        lastRow = ThisWorkbook.Sheets("Summary").Cells(Rows.Count, 1).End(xlUp).Row

   

        

 

        With wsinfo

            connectionname = "P52_DIGITAL_ERP_SYSTEM"

            login = Range("Q4").Value

            pass = Range("Q5").Value

           ' TGT = Range("J6").Value

            'tgt.Offset(1, 0).Select        'You have to move 1 cell down from your initial cell

           

        End With

       

     

    

        

        On Error Resume Next

       

    

        Shell "C:\Program Files (x86)\SAP\FrontEnd\SapGui\saplogon.exe", 4

        Set wshshell = CreateObject("wscript.shell")

        Do Until wshshell.AppActivate("sap logon")

        Application.Wait Now + TimeValue("0:00:01")

        Loop

        Set wshshell = Nothing

        Set sapgui = GetObject("sapgui")

        Set appl = sapgui.GetScriptingEngine

        Set connection = appl.openconnection(connectionname, True)

        Set session = connection.Children(0)

       

        session.findById("wnd[0]").maximize

        session.findById("wnd[0]/usr/txtRSYST-MANDT").Text = "452"

        session.findById("wnd[0]/usr/txtRSYST-BNAME").Text = login

        session.findById("wnd[0]/usr/pwdRSYST-BCODE").Text = pass

        session.findById("wnd[0]/usr/txtRSYST-LANGU").Text = "EN"

        session.findById("wnd[0]/usr/txtRSYST-LANGU").SetFocus

        session.findById("wnd[0]/usr/txtRSYST-LANGU").caretPosition = 2

        session.findById("wnd[0]").sendVKey 0

 

        If session.Children.Count > 1 Then

       

        session.findById("wnd[1]/usr/radMULTI_LOGON_OPT2").Select

        session.findById("wnd[1]/usr/radMULTI_LOGON_OPT2").SetFocus

        session.findById("wnd[1]/tbar[0]/btn[0]").press

        session.findById("wnd[1]/tbar[0]/btn[0]").press

        'Exit Sub

 

        End If

 

           On Error Resume Next

 

 

            session.findById("wnd[0]/tbar[0]/okcd").Text = "Zdecomrep"

            session.findById("wnd[0]").sendVKey 0

            session.findById("wnd[0]/tbar[1]/btn[17]").press

            session.findById("wnd[1]/usr/txtENAME-LOW").Text = "p50115368"

            session.findById("wnd[1]/usr/txtENAME-LOW").SetFocus

            session.findById("wnd[1]/usr/txtENAME-LOW").caretPosition = 9

            session.findById("wnd[1]/tbar[0]/btn[8]").press

            session.findById("wnd[1]/usr/cntlALV_CONTAINER_1/shellcont/shell").selectedRows = "0"

            session.findById("wnd[1]/usr/cntlALV_CONTAINER_1/shellcont/shell").doubleClickCurrentCell

            session.findById("wnd[0]/usr/txtS_VBELN-LOW").SetFocus

            session.findById("wnd[0]/usr/txtS_VBELN-LOW").caretPosition = 0

            session.findById("wnd[0]/usr/btn%_S_VBELN_%_APP_%-VALU_PUSH").press

            sapApp.SendKeys "^v"

            session.findById("wnd[1]/tbar[0]/btn[24]").press

            session.findById("wnd[1]/tbar[0]/btn[8]").press

            session.findById("wnd[0]/tbar[1]/btn[8]").press

            session.findById("wnd[0]/mbar/menu[0]/menu[3]/menu[1]").Select

            session.findById("wnd[1]/usr/ctxtDY_PATH").Text = "C:\Users\Priyabrata.T\Downloads\"

            session.findById("wnd[1]/usr/ctxtDY_FILENAME").Text = "ZDECOM.XLSX"

            session.findById("wnd[1]/usr/ctxtDY_FILENAME").caretPosition = 6

            session.findById("wnd[1]/tbar[0]/btn[11]").press

           

            session.findById("wnd[0]/mbar/menu[5]/menu[12]").Select

            session.findById("wnd[1]/usr/btnSPOP-OPTION1").press

           

            Workbooks("ZDECOM.xlsx").Close

           

            

End Sub