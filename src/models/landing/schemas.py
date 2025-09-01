"""
Landing models for COVID-19 data from different sources
"""
from datetime import date
from typing import List, Optional
from pydantic import BaseModel, Field, validator

class CasoDiarioLanding(BaseModel):
    """Landing model for daily COVID cases from Graph source"""
    fecha: date
    estado: str
    confirmados: Optional[int] = 0
    defunciones: Optional[int] = 0
    negativos: Optional[int] = 0
    sospechosos: Optional[int] = 0
    source_file: str

class CasoCovidLanding(BaseModel):
    """Landing model for individual COVID cases from Relational source"""
    id_registro: str
    fecha_actualizacion: date
    origen: str
    sector: str
    entidad_um: str
    sexo: str
    entidad_nac: str
    entidad_res: str
    municipio_res: str
    tipo_paciente: str
    fecha_ingreso: date
    fecha_sintomas: date
    fecha_def: Optional[date] = None
    intubado: Optional[bool] = None
    neumonia: Optional[bool] = None
    edad: int
    nacionalidad: str
    embarazo: Optional[bool] = None
    habla_lengua_indig: Optional[bool] = None
    diabetes: Optional[bool] = None
    epoc: Optional[bool] = None
    asma: Optional[bool] = None
    inmusupr: Optional[bool] = None
    hipertension: Optional[bool] = None
    otra_com: Optional[bool] = None
    cardiovascular: Optional[bool] = None
    obesidad: Optional[bool] = None
    renal_cronica: Optional[bool] = None
    tabaquismo: Optional[bool] = None
    otro_caso: Optional[bool] = None
    resultado: str
    migrante: Optional[bool] = None
    pais_nacionalidad: str
    pais_origen: str
    uci: Optional[bool] = None
    source_file: str
    
    @validator('intubado', 'neumonia', 'embarazo', 'habla_lengua_indig',
              'diabetes', 'epoc', 'asma', 'inmusupr', 'hipertension',
              'otra_com', 'cardiovascular', 'obesidad', 'renal_cronica',
              'tabaquismo', 'otro_caso', 'migrante', 'uci', pre=True)
    def validate_boolean(cls, v):
        """Convierte valores numéricos a booleanos
        1: Sí (True)
        2: No (False)
        97/98/99: No especificado (None)
        """
        if v in [97, 98, 99]:
            return None
        return v == 1

class TextoCasoLanding(BaseModel):
    """Landing model for COVID cases from text files"""
    texto_completo: str
    fecha_extraccion: date
    entidad: str
    keywords: List[str]
    source_file: str
