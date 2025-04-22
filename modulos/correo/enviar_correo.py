import win32com.client
import pandas as pd
import os

def enviocorreo(tabla):
    dir        = os.getcwd()
    directorio = dir +'\\archivocsv\\'
    ############################################################
    if tabla == 'audit':
        macro = 'EnviarMailaudit'
    elif tabla == 'trans':
        macro = 'EnviarMailTrans'
    else:
        macro = 'Enviarnoaplica'

    runmacro = f'''correo.xlsm!{macro}'''
    try:
        # Create a DataFrame
        df = pd.read_csv(directorio+'tabla_actualizada.csv', sep = ';')
        # Open an Excel file
        writer = pd.ExcelWriter(directorio+'tabla_actualizada.xlsx')
        # Write the DataFrame to the Excel file
        df.to_excel(writer)
        # # Save the Excel file
        writer.close()
        # ############################################################
        excelmacro = f'''{directorio}correo.xlsm'''
        
        # ############################################################
        try:
            xl = win32com.client.dynamic.Dispatch('Excel.Application')
            xl.Workbooks.Open(Filename = excelmacro, ReadOnly= 1)
            xl.Application.Run(runmacro)
            xl.Workbooks(1).Close(SaveChanges=1)
            xl.Application.Quit()
            del xl

        except Exception as e:
            print(e)

    except Exception as e:
        print(e)

# trans
#tabla = 'trans'
#enviocorreo(tabla)

