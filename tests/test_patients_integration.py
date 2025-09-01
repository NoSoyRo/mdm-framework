"""
Test suite para la integración de pacientes (Práctica 1)
"""
import pytest
from pathlib import Path
from src.etl.patients_integration import (
    actividad2_cargar_siglo21,
    actividad3_agregar_abc,
    actividad4_agregar_medica_sur,
    actividad5_agregar_gpo_angeles,
    actividad6_estadisticas,
    normalize_text,
    TABLA_MAPEO
)

@pytest.fixture
def p1_path():
    """Ruta a los archivos de la práctica 1"""
    return Path(__file__).parent.parent / "p1"

def test_actividad1_mapeo():
    """Verifica la tabla de mapeo de campos entre fuentes"""
    assert TABLA_MAPEO[0]['Siglo21'] == 'NOMBRE'
    assert TABLA_MAPEO[0]['ABC'] == 'NOMBRE'
    assert TABLA_MAPEO[0]['PacienteFederado'] == 'nombrePac'
    # Verificar estructura completa del mapeo
    campos_requeridos = {'Siglo21', 'ABC', 'MedicaSur', 'GpoAngeles', 'PacienteFederado'}
    for mapping in TABLA_MAPEO:
        assert set(mapping.keys()) == campos_requeridos

def test_normalize_text():
    """Prueba la normalización de texto"""
    assert normalize_text("José") == "JOSE"
    assert normalize_text("Martínez") == "MARTINEZ"
    assert normalize_text(" Calle 123 ") == "CALLE 123"
    assert normalize_text(None) is None
    assert normalize_text("áéíóúñ") == "AEIOUN"

def test_actividad2_siglo21(p1_path):
    """Prueba la carga de datos del Hospital Siglo 21"""
    pacientes = actividad2_cargar_siglo21(p1_path)
    assert len(pacientes) > 0
    # Verificar que no hay duplicados por pac_clave
    claves = set(p.pac_clave for p in pacientes)
    assert len(claves) == len(pacientes)
    # Verificar formato de datos
    for p in pacientes:
        assert p.nombrePac.isupper()
        assert p.apePatPac.isupper() if p.apePatPac else True
        assert isinstance(p.pac_clave, int)
        assert p.HospOrigen == 'Siglo21'

def test_actividad3_abc(p1_path):
    """Prueba la integración de datos del Hospital ABC"""
    # Primero cargar Siglo21
    siglo21 = actividad2_cargar_siglo21(p1_path)
    # Luego agregar ABC
    todos = actividad3_agregar_abc(p1_path, siglo21)
    assert len(todos) > len(siglo21)  # Debería haber más registros
    
    # Verificar que no hay duplicados
    claves = set((p.pac_clave, p.nombrePac, p.apePatPac) for p in todos)
    assert len(claves) == len(todos)
    
    # Verificar que hay registros de ambos hospitales
    origenes = {p.HospOrigen for p in todos}
    assert 'Siglo21' in origenes
    assert 'ABC' in origenes

def test_actividad4_medica_sur(p1_path):
    """Prueba la integración de datos de Medica Sur"""
    # Cargar datos previos
    previos = actividad3_agregar_abc(p1_path, actividad2_cargar_siglo21(p1_path))
    # Agregar Medica Sur
    todos = actividad4_agregar_medica_sur(p1_path, previos)
    assert len(todos) > len(previos)
    
    # Verificar hospital nuevo
    origenes = {p.HospOrigen for p in todos}
    assert 'MedicaSur' in origenes
    
    # Verificar formato de datos de nuevos registros
    medica_sur = [p for p in todos if p.HospOrigen == 'MedicaSur']
    assert len(medica_sur) > 0
    for p in medica_sur:
        assert p.nombrePac.isupper()
        assert p.apePatPac.isupper() if p.apePatPac else True
        assert isinstance(p.pac_clave, int)

def test_actividad5_gpo_angeles(p1_path):
    """Prueba la integración de datos del Grupo Angeles"""
    # Cargar datos previos
    previos = actividad4_agregar_medica_sur(
        p1_path,
        actividad3_agregar_abc(p1_path, actividad2_cargar_siglo21(p1_path))
    )
    # Agregar Grupo Angeles
    todos = actividad5_agregar_gpo_angeles(p1_path, previos)
    assert len(todos) > len(previos)
    
    # Verificar hospital nuevo
    origenes = {p.HospOrigen for p in todos}
    assert 'GpoAngeles' in origenes
    
    # Verificar formato de datos nuevos
    gpo_angeles = [p for p in todos if p.HospOrigen == 'GpoAngeles']
    assert len(gpo_angeles) > 0
    for p in gpo_angeles:
        assert p.nombrePac.isupper()
        assert p.apePatPac.isupper() if p.apePatPac else True
        assert isinstance(p.pac_clave, int)

def test_actividad6_estadisticas(p1_path):
    """Prueba el cálculo de estadísticas de integración"""
    # Cargar todos los datos
    todos = actividad5_agregar_gpo_angeles(
        p1_path,
        actividad4_agregar_medica_sur(
            p1_path,
            actividad3_agregar_abc(p1_path, actividad2_cargar_siglo21(p1_path))
        )
    )
    
    stats = actividad6_estadisticas(todos)
    
    # Verificar estructura de estadísticas
    assert 'total_pacientes' in stats
    assert 'por_hospital' in stats
    assert 'duplicados_entre_hospitales' in stats
    assert 'campos_vacios' in stats
    
    # Verificar conteos
    assert stats['total_pacientes'] == len(todos)
    assert sum(stats['por_hospital'].values()) == len(todos)
    assert all(hosp in stats['por_hospital'] for hosp in 
              ['Siglo21', 'ABC', 'MedicaSur', 'GpoAngeles'])
    
    # Verificar campos vacíos
    for campo in ['nombrePac', 'apePatPac', 'direccion']:
        assert campo in stats['campos_vacios']
        assert isinstance(stats['campos_vacios'][campo], int)
        assert stats['campos_vacios'][campo] >= 0
