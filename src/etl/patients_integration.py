"""Integración de pacientes (Práctica 1 Actividades 1-6)
Actividad 1: Exploración y mapeo de campos entre fuentes
Actividad 2: Cargar Hospital Siglo 21 (MySQL dump SQL)
Actividad 3: Cargar Hospital ABC (JSON) evitando duplicados
Actividad 4: Agregar Medica Sur (CSV)
Actividad 5: Agregar Grupo Angeles (Excel)
Actividad 6: Estadísticas de integración
"""
from __future__ import annotations
from pathlib import Path
import json
import re
import pandas as pd
from typing import List, Dict, Optional, Tuple
from ..models.landing.schemas import PacienteFederadoLanding

# --- Utilidades de normalización ---
_accent_map = str.maketrans('ÁÉÍÓÚÜÑáéíóúüñ', 'AEIOUUNAEIOUUN')

def normalize_text(s: str) -> str:
    if s is None:
        return None
    s = str(s).strip()
    s = s.translate(_accent_map)
    s = re.sub(r'\s+', ' ', s)
    return s.upper()

# --- Actividad 1: Mapeo de campos ---
# Definimos el diccionario de mapeo lógico hacia el modelo PacienteFederadoLanding
# clave: nombre en archivo origen -> (campo destino, transform)
MAPPING_SIGLO21 = {
    'NOMBRE': ('nombrePac', normalize_text),
    'APELLIDO': ('apePatPac', normalize_text),  # Asumimos solo un apellido en origen
    'NSS': ('pac_clave', None),  # Será convertido a int seguro
    'Direccion': ('direccion', normalize_text)
}

MAPPING_ABC = {
    'NOMBRE': ('nombrePac', normalize_text),
    'APELLIDO': ('apePatPac', normalize_text),
    'NSS': ('pac_clave', None),
    'Direccion': ('direccion', normalize_text)
}

# Nota: Medica Sur CSV aún no se procesa (Actividad 4)

# --- Lectores de fuentes ---

def load_siglo21_sql(path: Path) -> pd.DataFrame:
    sql_text = path.read_text(encoding='utf-8', errors='ignore')
    # Extraer los bloques INSERT
    rows = []
    for match in re.finditer(r'INSERT INTO `Pacientes` .*?VALUES\s*(.*?);', sql_text, flags=re.DOTALL):
        values_part = match.group(1)
        # Separar tuples
        tuples = re.findall(r'\((.*?)\)', values_part, flags=re.DOTALL)
        for tup in tuples:
            # Campos entre comillas separadas por "," fuera de comillas internas simples
            raw_fields = re.findall(r'"(.*?)"', tup)
            if len(raw_fields) == 4:
                rows.append(dict(NOMBRE=raw_fields[0], APELLIDO=raw_fields[1], NSS=raw_fields[2], Direccion=raw_fields[3]))
    return pd.DataFrame(rows)

def load_abc_json(path: Path) -> pd.DataFrame:
    data = json.loads(path.read_text(encoding='utf-8'))
    return pd.DataFrame(data)

# --- Transformaciones comunes ---

def apply_mapping(df: pd.DataFrame, mapping: Dict[str, tuple], origen: str) -> List[PacienteFederadoLanding]:
    pacientes: List[PacienteFederadoLanding] = []
    for _, row in df.iterrows():
        record = {
            'pac_clave': None,
            'nombrePac': None,
            'apePatPac': None,
            'apeMatPac': None,
            'direccion': None,
            'HospOrigen': origen
        }
        for src_col, (dest_col, transform) in mapping.items():
            val = row.get(src_col)
            if transform:
                val = transform(val)
            record[dest_col] = val
        # Convertir NSS a entero truncando caracteres no numéricos
        if record['pac_clave'] is not None:
            digits = re.sub(r'\D', '', str(record['pac_clave']))
            if digits:
                record['pac_clave'] = int(digits[:8])  # limitar a 8 como especificación int(8)
        # Crear surrogate si falta
        if record['pac_clave'] is None:
            continue
        try:
            pacientes.append(PacienteFederadoLanding(**record))
        except Exception:
            continue
    return pacientes

# --- Eliminación de duplicados ---

def deduplicate(pacientes: List[PacienteFederadoLanding]) -> List[PacienteFederadoLanding]:
    seen = {}
    for p in pacientes:
        key = (p.pac_clave, p.nombrePac, p.apePatPac)
        if key not in seen:
            seen[key] = p
    return list(seen.values())

# --- Funciones públicas para actividades ---

def actividad2_cargar_siglo21() -> List[PacienteFederadoLanding]:
    df = load_siglo21_sql(FUENTES_DIR / 'PacientesSiglo21-mysql.sql')
    pacientes = apply_mapping(df, MAPPING_SIGLO21, 'Siglo21')
    return deduplicate(pacientes)

def actividad3_agregar_abc(pacientes_existentes: List[PacienteFederadoLanding]) -> List[PacienteFederadoLanding]:
    df = load_abc_json(FUENTES_DIR / 'PacientesHospitalABC.json')
    nuevos = apply_mapping(df, MAPPING_ABC, 'ABC')
    combinados = pacientes_existentes + nuevos
    return deduplicate(combinados)

def actividad4_agregar_medica_sur(p1_path: Path, pacientes_previos: List[PacienteFederadoLanding]) -> List[PacienteFederadoLanding]:
    """Integra datos del Hospital Medica Sur desde CSV.
    
    Mapeo:
    - NoPaciente (num) -> pac_clave
    - NombreCompleto (split) -> nombrePac, apePatPac
    - ubicacion -> direccion
    """
    medica_sur_path = p1_path / 'PacientesMedicaSurCSV.csv'
    df = pd.read_csv(medica_sur_path)
    
    existentes = {(p.pac_clave, p.nombrePac, p.apePatPac) for p in pacientes_previos}
    nuevos = []
    
    for _, row in df.iterrows():
        try:
            # Separar nombre completo
            nombre_completo = normalize_text(row['NombreCompleto']).split()
            if len(nombre_completo) < 2:
                continue
                
            nombre = nombre_completo[0]
            apellido = nombre_completo[1]
            
            # Extraer número de paciente
            no_paciente = extract_nss(row['NoPaciente'])
            
            pac = PacienteFederadoLanding(
                pac_clave=no_paciente,
                nombrePac=nombre,
                apePatPac=apellido,
                direccion=normalize_text(row['ubicacion']),
                HospOrigen='MedicaSur'
            )
            
            key = (pac.pac_clave, pac.nombrePac, pac.apePatPac)
            if key not in existentes:
                nuevos.append(pac)
                existentes.add(key)
                
        except (KeyError, ValueError) as e:
            print(f"Error procesando registro Medica Sur: {e}")
            continue
            
    return pacientes_previos + nuevos

def actividad5_agregar_gpo_angeles(p1_path: Path, pacientes_previos: List[PacienteFederadoLanding]) -> List[PacienteFederadoLanding]:
    """Integra datos del Hospital Grupo Angeles desde Excel."""
    angeles_path = p1_path / 'PacientesGpoAngeles-excel.xlsx'
    df = pd.read_excel(angeles_path)
    
    existentes = {(p.pac_clave, p.nombrePac, p.apePatPac) for p in pacientes_previos}
    nuevos = []
    
    for _, row in df.iterrows():
        try:
            pac = PacienteFederadoLanding(
                pac_clave=extract_nss(str(row['IdPaciente'])),
                nombrePac=normalize_text(row['Nombre']),
                apePatPac=normalize_text(row['ApellidoPaterno']),
                direccion=normalize_text(row['Direccion']),
                HospOrigen='GpoAngeles'
            )
            
            key = (pac.pac_clave, pac.nombrePac, pac.apePatPac)
            if key not in existentes:
                nuevos.append(pac)
                existentes.add(key)
                
        except (KeyError, ValueError) as e:
            print(f"Error procesando registro Grupo Angeles: {e}")
            continue
            
    return pacientes_previos + nuevos

def actividad6_estadisticas(pacientes: List[PacienteFederadoLanding]) -> Dict[str, any]:
    """Calcula estadísticas sobre la integración de datos."""
    stats = {
        'total_pacientes': len(pacientes),
        'por_hospital': {},
        'duplicados_entre_hospitales': 0,
        'campos_vacios': {
            'nombrePac': 0,
            'apePatPac': 0,
            'direccion': 0
        }
    }
    
    # Conteo por hospital
    for p in pacientes:
        stats['por_hospital'][p.HospOrigen] = stats['por_hospital'].get(p.HospOrigen, 0) + 1
        
        # Conteo campos vacíos
        if not p.nombrePac:
            stats['campos_vacios']['nombrePac'] += 1
        if not p.apePatPac:
            stats['campos_vacios']['apePatPac'] += 1
        if not p.direccion:
            stats['campos_vacios']['direccion'] += 1
    
    # Buscar duplicados entre hospitales
    claves_por_hospital: Dict[int, set] = {}
    for p in pacientes:
        if p.pac_clave not in claves_por_hospital:
            claves_por_hospital[p.pac_clave] = set()
        claves_por_hospital[p.pac_clave].add(p.HospOrigen)
    
    # Contar claves que aparecen en más de un hospital
    stats['duplicados_entre_hospitales'] = sum(1 for hospitales in claves_por_hospital.values() 
                                             if len(hospitales) > 1)
    
    return stats

if __name__ == '__main__':
    base = Path(__file__).parent.parent.parent
    p1_path = base / 'p1'
    
    print("\n=== Actividad 2: Carga Siglo21 ===")
    siglo21 = actividad2_cargar_siglo21(p1_path)
    print(f"Registros cargados de Siglo21: {len(siglo21)}")
    
    print("\n=== Actividad 3: Integración ABC ===")
    con_abc = actividad3_agregar_abc(p1_path, siglo21)
    print(f"Total registros después de ABC: {len(con_abc)}")
    
    print("\n=== Actividad 4: Integración Medica Sur ===")
    con_medica = actividad4_agregar_medica_sur(p1_path, con_abc)
    print(f"Total registros después de Medica Sur: {len(con_medica)}")
    
    print("\n=== Actividad 5: Integración Grupo Angeles ===")
    todos = actividad5_agregar_gpo_angeles(p1_path, con_medica)
    print(f"Total registros finales: {len(todos)}")
    
    print("\n=== Actividad 6: Estadísticas ===")
    stats = actividad6_estadisticas(todos)
    print("\nEstadísticas de integración:")
    print(f"Total pacientes: {stats['total_pacientes']}")
    print("\nPor hospital:")
    for hosp, count in stats['por_hospital'].items():
        print(f"- {hosp}: {count}")
    print(f"\nDuplicados entre hospitales: {stats['duplicados_entre_hospitales']}")
    print("\nCampos vacíos:")
    for campo, count in stats['campos_vacios'].items():
        print(f"- {campo}: {count}")
