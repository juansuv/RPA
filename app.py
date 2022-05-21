import pandas as pd
import os
import io, base64
from io import StringIO
from fastapi import FastAPI, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from clean import clean
from validated import validated
from proccess import proccess


def fastApp() -> FastAPI:
    app = FastAPI(title="PA", description="Asignación de causales automático")
    
    origins = [
    "http://localhost.",
    "https://localhost",
    "http://localhost",
    "http://localhost:3000"
]


    app.add_middleware(
    CORSMiddleware,
    allow_origins= origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    )
    return app

app = fastApp()


@app.post('/uploadfile/')
async def asignacion_causales(
        experiment: str = Form(...),
        file_type: str = Form(...),
        file_id: str = Form(...),
        data_file: UploadFile = File(...),
        ):
    
    #decoded = base64.b64decode(data_file.file)
    #decoded = io.StringIO(decoded.decode('utf-8'))
    data_temp=data_file.file.read()
   

    data_analisis = pd.read_excel(data_temp, sheet_name=f"27-04-2022", header=0)
    Moviemientos = pd.read_excel(data_temp, sheet_name="MOV", header=0)
    compras = pd.read_excel(data_temp, sheet_name="COMPRAS_1", header=0)
    pedidos = pd.read_excel(data_temp, sheet_name="PEDIDOS", header=0)
    Datos = pd.read_excel(data_temp, sheet_name="Datos", header=0) 
    print("cargo_Data")
    data_validada=validated(data_file)
    data_limpia=clean(data_validada)    
    data_ejecutada=proccess(data_analisis,Moviemientos,compras)

    return {'filename': data_file.filename, 
            'experiment':experiment, 
            'file_type': file_type, 
            'file_id': file_id}