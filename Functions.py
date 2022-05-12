import pandas as pd
import numpy as np
import datetime as dt

'''
@author Juanes
'''


class asignacion_causales:
    def __init__(self):
        print("Asignacion de causales")

    def leer_archivo(self, file, sheet: int):
        df_causales = pd.read_excel(file.file.read(), sheet_name=sheet, header=0)
        return pd.DataFrame(df_causales)

    def asignar_causal24(self, file):
        file["Impor"] = file["Impor"].astype(np.int64)
        file["Novedad Cadena"] = file["Novedad Cadena"].astype(np.int64)
        file["Inventario Cierre"] = file["Inventario Cierre"].astype(np.int64)
        file.loc[(np.logical_or(file["Impor"]!=0,
            np.logical_and(file["Id_Medicion"] == file["Id_Medicion"], file["Novedad Cadena"] != 1))),"Novedad Cadena"] = 24
        print("Causal 24",file[np.logical_or(file["Impor"]!=0,
            np.logical_and(file["Id_Medicion"] == file["Id_Medicion"], file["Novedad Cadena"] != 1))])
        return file

    def asignar_causal5(self, file):
        data = file[file["Novedad"] == 41]
        data.reset_index(drop=True, inplace=True)
        data.loc[(np.logical_and(data["Novedad Cadena"] == 1, np.logical_or(data["Inventario Cierre"] < 0, np.logical_and(data["Inventario Cierre"] >= 3, np.logical_and(
            data["Ventas Post"] < data.iat[0, 8], data["Ingresos"] < data.iat[0, 8]))), np.logical_and(data["Inventario Cierre"] == 0, data["Ajustes"] < data.iat[0, 8]-dt.timedelta(7)))),"Novedad Cadena"] = 5
        
        print("Causal 5",data[np.logical_and(data["Novedad Cadena"] == 1, np.logical_or(data["Inventario Cierre"] < 0, np.logical_and(data["Inventario Cierre"] >= 3, np.logical_and(
            data["Ventas Post"] < data.iat[0, 8], data["Ingresos"] < data.iat[0, 8]))), np.logical_and(data["Inventario Cierre"] == 0, data["Ajustes"] < data.iat[0, 8]-dt.timedelta(7)))])
        
        return data

    def merge_causal4(self, file, hoja_mov):
        hoja_mov.columns = hoja_mov.columns.str.strip()
        hoja_mov["CLAVE"] = hoja_mov["CLAVE"].fillna(0).astype(np.int64)
        file = file.merge(hoja_mov[["Cantidad", "CLAVE"]], how='left',
                          left_on='CLAVE', right_on='CLAVE', indicator=True)
             
        file[np.logical_and(file["Impor"] != 0, file["Novedad Cadena"] == 1, np.logical_and(file["Inventario Cierre"] == 2, file["Ingresos"]
                                < file.iat[0, 8]-dt.timedelta(7)))]["Observaciones"] = f"Ajuste de inventario {file.iat[0,8]} en {file['Cantidad']}"
        return file

    def merge_causal11_8(self, file, hojaMov,hojaCompras):
        # hojaMov = self.leer_archivo(nombre_archivo, 1)
        # hojaCompras = self.leer_archivo(nombre_archivo, 2)
        hojaMov["CLAVE"] = hojaMov["CLAVE"].fillna(0).astype(np.int64)
        hojaCompras["Llave"] = hojaCompras["Llave"].fillna(0).astype(np.int64)
        hojaMov.columns = hojaMov.columns.str.strip()
        hojaCompras.columns = hojaCompras.columns.str.strip()
        file = file.merge(hojaMov[["Cantidad", "Fecha doc.", "FechaEntr",
                          "CMv", "CLAVE"]], how='left', on='CLAVE', indicator="exists_mov")
        file = file.merge(hojaCompras[["Cantidad", "Fecha doc.", "Doc.compr.", "Fe.entrega", "Llave"]],
                          how='left', left_on='CLAVE', right_on="Llave", indicator="exists_compra")

        file['Fecha doc._x'] =file['Fecha doc._x'].fillna('12-12-2022')
        file['Fecha doc._x']=file['Fecha doc._x'].astype(str).str.isdigit()        
        file['Fecha doc._x'] = pd.to_datetime(file['Fecha doc._x'],errors='coerce')
        
        file['Fecha doc._y'] =file['Fecha doc._y'].fillna('12-12-2022')
        file['Fecha doc._y']=file['Fecha doc._y'].astype(str).str.isdigit()        
        file['Fecha doc._y'] = pd.to_datetime(file['Fecha doc._y'],errors='coerce')
        
        
        file['Fe.entrega'] =file['Fe.entrega'].fillna('12-12-2022')
        file['Fe.entrega']=file['Fe.entrega'].astype(str).str.isdigit()        
        file['Fe.entrega'] = pd.to_datetime(file['Fe.entrega'],errors='coerce')
        
        file['FechaEntr'] =file['FechaEntr'].fillna('12-12-2022')
        file['FechaEntr']=file['FechaEntr'].astype(str).str.isdigit()        
        file['FechaEntr'] = pd.to_datetime(file['FechaEntr'],errors='coerce')
        return file

    def asignar_causal4(self, file, file_):
        data = file[file["Novedad"] == 41]
        file = file[file["Novedad"] != 41]
        data.reset_index(drop=True, inplace=True)
        data[np.logical_and(data["Impor"] != 0, data["Novedad Cadena"] == 1, np.logical_or(
            data["Ventas Post"] > data.iat[0, 8], data["Ingresos"] < data.iat[0, 8]-dt.timedelta(7)))]["Novedad cadena"] = 4
        print("Causal 4 ",data[np.logical_and(data["Impor"] != 0, data["Novedad Cadena"] == 1, np.logical_or(
            data["Ventas Post"] > data.iat[0, 8], data["Ingresos"] < data.iat[0, 8]-dt.timedelta(7)))])
        data[np.logical_and(data["Impor"] != 0, data["Novedad Cadena"] == 1, np.logical_or(
            data["Ventas Post"] > data.iat[0, 8], data["Ingresos"] >= data.iat[0, 8]))]["Observaciones"] = "Venta posterior"
        data.reset_index(drop=True, inplace=True)
        data_observacion = data[np.logical_and(data["Impor"] != 0, data["Novedad Cadena"] == 1, np.logical_and(
            data["Inventario Cierre"] == 2, data["Ingresos"] < data.iat[0, 8]-dt.timedelta(7)))]
        data = data[np.logical_and(data["Impor"] == 0, data["Novedad Cadena"] != 1, np.logical_and(
            data["Inventario Cierre"] != 2, data["Ingresos"] > data.iat[0, 8]-dt.timedelta(7)))]
        if len(data_observacion["Impor"])>0:
            data_observacion = self.merge_causal4(data_observacion, file_)
            data = data.append(data_observacion)
                
        file = file.append(data)
        return file

    def asignar_causal6(self, file):
        data = file[file["Novedad"] == 41]
        file = file[file["Novedad"] != 41]
        data.reset_index(drop=True, inplace=True)
        fecha_ = data["Fecha"][0]
        data.loc[(np.logical_and(data["Impor"] != 0, data["Novedad Cadena"] == 1, np.logical_and(fecha_ == data["Ingresos"], data["Inventario Cierre"]
        < data["Cnt Entrega"],np.logical_and(data["U Pedido"] < fecha_, data["U Pedido"] >= fecha_-dt.timedelta(weeks=4))))),"Novedad Cadena"] = 6
        
        print("Causal 6 ",data[np.logical_and(data["Impor"] != 0, data["Novedad Cadena"] == 1, np.logical_and(fecha_ == data["Ingresos"], data["Inventario Cierre"]
        < data["Cnt Entrega"],np.logical_and(data["U Pedido"] < fecha_, data["U Pedido"] >= fecha_-dt.timedelta(weeks=4))))])
        
        file = file.append(data)
        return file

    def asignar_causal8(self, file):
        data = file[file["Novedad"] != 41]
        file = file[file["Novedad"] == 41]
        data.reset_index(drop=True, inplace=True)
        file.reset_index(drop=True, inplace=True)
        fecha_ = file["Fecha"][0]
        file.loc[(np.logical_and(file["Impor"] == 0, file["Novedad Cadena"] == 1, np.logical_and(file["Fecha doc._x"] < fecha_,np.logical_and(np.logical_and(file["Fecha doc._x"] <= (
            fecha_-dt.timedelta(weeks=3)), file["Fecha doc._x"] <= (fecha_-dt.timedelta(weeks=4))), np.logical_and(file["Cantidad_x"] == file["Cantidad_y"], 
            file["Fe.entrega"] < file["FechaEntr"]))))),"Novedad Cadena"] = 8
        print("Causal 8 ",file[np.logical_and(file["Impor"] == 0, file["Novedad Cadena"] == 1, np.logical_and(file["Fecha doc._x"] < fecha_,np.logical_and(np.logical_and(file["Fecha doc._x"] <= (
            fecha_-dt.timedelta(weeks=3)), file["Fecha doc._x"] <= (fecha_-dt.timedelta(weeks=4))), np.logical_and(file["Cantidad_x"] == file["Cantidad_y"], 
            file["Fe.entrega"] < file["FechaEntr"]))))])
        file = file.append(data)
        return file

    def asignar_causal11(self, file):
        data = file[file["Novedad"] != 41]
        file = file[file["Novedad"] == 41]
        file.reset_index(drop=True, inplace=True)
        fecha_ =file["Fecha"][0]
        
        file[np.logical_and(file["Impor"] == 0, file["Novedad Cadena"] == 1, np.logical_and(file["Fecha doc._x"] <= (fecha_-dt.timedelta(weeks=2)), file["Fe.entrega"] > fecha_, np.logical_and(np.logical_and(
            file["Fecha doc._x"] <= (fecha_-dt.timedelta(weeks=3)), file["Fecha doc._x"] <= (fecha_-dt.timedelta(weeks=4))), file["Fe.entrega"] > fecha_, file["Cantidad_x"] != file["Cantidad_y"])))]["Novedad Cadena"] = 11
        print("Causal 11 ",file[np.logical_and(file["Impor"] == 0, file["Novedad Cadena"] == 1, np.logical_and(file["Fecha doc._x"] <= (fecha_-dt.timedelta(weeks=2)), file["Fe.entrega"] > fecha_, np.logical_and(np.logical_and(
            file["Fecha doc._x"] <= (fecha_-dt.timedelta(weeks=3)), file["Fecha doc._x"] <= (fecha_-dt.timedelta(weeks=4))), file["Fe.entrega"] > fecha_, file["Cantidad_x"] != file["Cantidad_y"])))])
        file[np.logical_and(file["Impor"] == 0, file["Novedad Cadena"] == 1, np.logical_and(file["Fecha doc._x"] <= (fecha_-dt.timedelta(weeks=2)), np.logical_and(np.logical_and(
            file["Fecha doc._x"] <= (fecha_-dt.timedelta(weeks=3)), file["Fecha doc._x"] <= (fecha_-dt.timedelta(weeks=4))), file["Cantidad_x"] != file["Cantidad_y"])))]["Observaciones"] = "Despacho incompleto"
        file[np.logical_and(file["Impor"] == 0, file["Novedad Cadena"] == 1, np.logical_and(file["Fecha doc._x"] <= (fecha_-dt.timedelta(weeks=2)), np.logical_and(np.logical_and(file["Fecha doc._x"] <= (
            fecha_-dt.timedelta(weeks=3)), file["Fecha doc._x"] <= (fecha_-dt.timedelta(weeks=4))), file["Fe.entrega"] > fecha_, file["Cantidad_x"] != file["Cantidad_y"])))]["Observaciones"] = "Despacho tarde"
        file = file.append(data)
        return file

    def exportar_excel(self, file):
        fecha = str(file["Fecha"][0]).split(' ')[0]
        file.to_excel(
            f'Medición - Análisis Medición causal24 - {fecha}.xlsx', sheet_name=f'{fecha}', encoding='utf8', index=False)
