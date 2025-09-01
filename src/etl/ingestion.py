"""
Data ingestion module for COVID-19 data
"""
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from ..models.landing.schemas import CasoDiarioLanding, CasoCovidLanding, TextoCasoLanding

class CovidDataIngester:
    """Ingests COVID-19 data from multiple sources into landing models"""
    
    def __init__(self, data_path: str):
        self.data_path = Path(data_path)
        
    def ingest_graph_data(self) -> List[CasoDiarioLanding]:
        """Ingest data from CSV files in Graph folder"""
        casos_diarios = []
        graph_path = self.data_path / 'Graph'
        
        # Archivos a procesar y tipo de caso correspondiente
        files = {
            'confirmados': 'Casos_Diarios_Estado_Nacional_Confirmados_20230625.csv',
            'defunciones': 'Casos_Diarios_Estado_Nacional_Defunciones_20230625.csv',
            'negativos': 'Casos_Diarios_Estado_Nacional_Negativos_20230625.csv',
            'sospechosos': 'Casos_Diarios_Estado_Nacional_Sospechosos_20230625.csv'
        }
        
        # Diccionario para almacenar datos por fecha y estado
        datos_por_fecha_estado: Dict[tuple, Dict] = {}
        
        # Procesar cada archivo
        for tipo_caso, filename in files.items():
            df = pd.read_csv(graph_path / filename)
            
            # Obtener las columnas que son fechas (excluyendo 'cve_ent', 'poblacion', 'nombre')
            fecha_cols = [col for col in df.columns if col not in ['cve_ent', 'poblacion', 'nombre']]
            
            # Procesar cada estado
            for _, row in df.iterrows():
                estado = row['nombre']
                
                # Procesar cada columna de fecha
                for fecha_str in fecha_cols:
                    casos = row[fecha_str]
                    # Convertir la fecha de DD-MM-YYYY a objeto date
                    fecha = datetime.strptime(fecha_str, '%d-%m-%Y').date()
                    
                    key = (fecha, estado)
                    if key not in datos_por_fecha_estado:
                        datos_por_fecha_estado[key] = {
                            'fecha': fecha,
                            'estado': estado,
                            'confirmados': 0,
                            'defunciones': 0,
                            'negativos': 0,
                            'sospechosos': 0,
                            'source_file': filename
                        }
                    
                    # Actualizar el conteo correspondiente según el tipo de caso
                    datos_por_fecha_estado[key][tipo_caso] = casos
        
        # Convertir a modelos de landing
        for datos in datos_por_fecha_estado.values():
            casos_diarios.append(CasoDiarioLanding(**datos))
        
        return casos_diarios
    
    def ingest_relational_data(self) -> List[CasoCovidLanding]:
        """Ingest data from COVID19MEXICO.csv"""
        casos = []
        rel_path = self.data_path / 'Relational' / 'COVID19MEXICO.csv'
        
        # Leer el CSV con pandas
        df = pd.read_csv(rel_path)
        
        def parse_date(date_str):
            """Convertir string de fecha a objeto date, retorna None si la fecha es inválida"""
            if pd.isna(date_str) or date_str == '9999-99-99':
                return None
            try:
                fecha = datetime.strptime(date_str, '%Y-%m-%d').date()
                # Si la fecha está en el futuro, es probablemente un error
                if fecha > datetime.now().date():
                    return None
                return fecha
            except ValueError:
                return None
        
        # Convertir cada fila a modelo de landing
        for _, row in df.iterrows():
            try:
                # Convertir fechas con manejo de valores inválidos
                fecha_act = parse_date(row['FECHA_ACTUALIZACION'])
                if not fecha_act:  # Si no hay fecha de actualización válida, skipear el caso
                    continue
                    
                fecha_ing = parse_date(row['FECHA_INGRESO'])
                if not fecha_ing:  # Si no hay fecha de ingreso válida, skipear el caso
                    continue
                    
                fecha_sin = parse_date(row['FECHA_SINTOMAS'])
                if not fecha_sin:  # Si no hay fecha de síntomas válida, skipear el caso
                    continue
                    
                # Fecha defunción puede ser None
                fecha_def = parse_date(row['FECHA_DEF']) if pd.notna(row['FECHA_DEF']) else None
                
                caso = CasoCovidLanding(
                    id_registro=str(row['ID_REGISTRO']),
                    fecha_actualizacion=fecha_act,
                    origen=str(row['ORIGEN']),
                    sector=str(row['SECTOR']),
                    entidad_um=str(row['ENTIDAD_UM']),
                    sexo=str(row['SEXO']),
                    entidad_nac=str(row['ENTIDAD_NAC']),
                    entidad_res=str(row['ENTIDAD_RES']),
                    municipio_res=str(row['MUNICIPIO_RES']),
                    tipo_paciente=str(row['TIPO_PACIENTE']),
                    fecha_ingreso=fecha_ing,
                    fecha_sintomas=fecha_sin,
                    fecha_def=fecha_def,
                    intubado=row['INTUBADO'],
                    neumonia=row['NEUMONIA'],
                    edad=int(row['EDAD']),
                    nacionalidad=str(row['NACIONALIDAD']),
                    embarazo=row['EMBARAZO'],
                    habla_lengua_indig=row['HABLA_LENGUA_INDIG'],
                    diabetes=row['DIABETES'],
                    epoc=row['EPOC'],
                    asma=row['ASMA'],
                    inmusupr=row['INMUSUPR'],
                    hipertension=row['HIPERTENSION'],
                    otra_com=row['OTRA_COM'],
                    cardiovascular=row['CARDIOVASCULAR'],
                    obesidad=row['OBESIDAD'],
                    renal_cronica=row['RENAL_CRONICA'],
                    tabaquismo=row['TABAQUISMO'],
                    otro_caso=row['OTRO_CASO'],
                    resultado=str(row['RESULTADO_PCR']) if pd.notna(row['RESULTADO_PCR']) else str(row['RESULTADO_ANTIGENO']),
                    migrante=row['MIGRANTE'],
                    pais_nacionalidad=str(row['PAIS_NACIONALIDAD']),
                    pais_origen=str(row['PAIS_ORIGEN']),
                    uci=row['UCI'],
                    source_file='COVID19MEXICO.csv'
                )
                casos.append(caso)
            except (ValueError, KeyError) as e:
                # Log error y continuar con el siguiente caso
                print(f"Error procesando caso: {e}")
                continue
        
        return casos
    
    def ingest_text_data(self) -> List[TextoCasoLanding]:
        """Ingest data from text files"""
        casos_texto = []
        text_path = self.data_path / 'Text' / 'data_descriptor.txt'
        
        # Leer archivo de texto
        with open(text_path, 'r', encoding='utf-8') as f:
            texto = f.read()
            
            # Ejemplo simple de extracción de keywords
            keywords = [word.strip() for word in texto.split() 
                       if len(word.strip()) > 5][:10]  # primeras 10 palabras largas
            
            caso = TextoCasoLanding(
                texto_completo=texto,
                fecha_extraccion=datetime.now().date(),
                entidad='Nacional',  # Se podría mejorar con NLP
                keywords=keywords,
                source_file='data_descriptor.txt'
            )
            casos_texto.append(caso)
        
        return casos_texto
