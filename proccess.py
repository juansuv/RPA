from Functions import asignacion_causales as causal

def proccess(archivo_medicion,Moviemientos,Compras):
    nombre_archivo="6-04-2022.xlsx"
    causales = causal()
    #archivo_medicion = causales.leer_archivo(data,0)
    #archivoMedicionHoja2=causales.leer_archivo(data,1)
    df_analisis=causales.asignar_causal24(archivo_medicion)
    df_analisis=causales.asignar_causal4(df_analisis,Moviemientos)
    df_analisis=causales.asignar_causal5(df_analisis)
    df_analisis=causales.asignar_causal6(df_analisis)
    df_analisis=causales.merge_causal11_8(df_analisis,Moviemientos,Compras)
    df_analisis=causales.asignar_causal8(df_analisis)
    df_analisis=causales.asignar_causal11(df_analisis)
    causales.exportar_excel(df_analisis)
