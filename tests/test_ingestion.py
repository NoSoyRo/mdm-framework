"""
Tests for data ingestion and landing models
"""
import pytest
from datetime import datetime, date
from pathlib import Path
from src.models.landing.schemas import CasoDiarioLanding, CasoCovidLanding, TextoCasoLanding
from src.etl.ingestion import CovidDataIngester

@pytest.fixture
def data_path():
    return Path(__file__).parent.parent / "data"

@pytest.fixture
def ingester(data_path):
    return CovidDataIngester(str(data_path))

def test_caso_diario_model():
    """Test CasoDiarioLanding model"""
    caso = CasoDiarioLanding(
        fecha=date(2023, 6, 25),
        estado="CIUDAD DE MEXICO",
        confirmados=100,
        defunciones=10,
        negativos=50,
        sospechosos=20,
        source_file="test.csv"
    )
    assert caso.estado == "CIUDAD DE MEXICO"
    assert caso.confirmados == 100
    assert isinstance(caso.fecha, date)

def test_caso_covid_model():
    """Test CasoCovidLanding model"""
    caso = CasoCovidLanding(
        id_registro="123",
        fecha_actualizacion=date(2023, 6, 25),
        origen="1",
        sector="12",
        entidad_um="01",
        sexo="2",
        entidad_nac="01",
        entidad_res="01",
        municipio_res="003",
        tipo_paciente="1",
        fecha_ingreso=date(2023, 6, 20),
        fecha_sintomas=date(2023, 6, 18),
        edad=45,
        nacionalidad="1",
        resultado="1",
        pais_nacionalidad="MEXICO",
        pais_origen="MEXICO",
        source_file="test.csv"
    )
    assert caso.id_registro == "123"
    assert caso.edad == 45
    assert isinstance(caso.fecha_actualizacion, date)
    
    # Probar validación de booleanos
    caso = CasoCovidLanding(
        id_registro="123",
        fecha_actualizacion=date(2023, 6, 25),
        origen="1",
        sector="12",
        entidad_um="01",
        sexo="2",
        entidad_nac="01",
        entidad_res="01",
        municipio_res="003",
        tipo_paciente="1",
        fecha_ingreso=date(2023, 6, 20),
        fecha_sintomas=date(2023, 6, 18),
        edad=45,
        nacionalidad="1",
        intubado=1,  # Sí
        neumonia=2,  # No
        embarazo=97,  # No especificado
        resultado="1",
        pais_nacionalidad="MEXICO",
        pais_origen="MEXICO",
        source_file="test.csv"
    )
    assert caso.intubado is True
    assert caso.neumonia is False
    assert caso.embarazo is None

def test_texto_caso_model():
    """Test TextoCasoLanding model"""
    caso = TextoCasoLanding(
        texto_completo="Este es un caso de COVID en CDMX",
        fecha_extraccion=date(2023, 6, 25),
        entidad="CIUDAD DE MEXICO",
        keywords=["COVID", "CDMX"],
        source_file="test.txt"
    )
    assert len(caso.keywords) == 2
    assert isinstance(caso.fecha_extraccion, date)

def test_ingest_graph_data(ingester):
    """Test ingestion of graph data"""
    casos = ingester.ingest_graph_data()
    assert len(casos) > 0
    assert all(isinstance(caso, CasoDiarioLanding) for caso in casos)
    
    # Verificar que tenemos datos para diferentes estados
    estados = {caso.estado for caso in casos}
    assert len(estados) > 1
    
    # Verificar los tipos de datos
    for caso in casos:
        assert isinstance(caso.fecha, date)
        assert isinstance(caso.estado, str)
        assert all(isinstance(x, int) for x in [caso.confirmados, caso.defunciones, 
                                              caso.negativos, caso.sospechosos])

def test_ingest_relational_data(ingester):
    """Test ingestion of relational data"""
    casos = ingester.ingest_relational_data()
    assert len(casos) > 0
    assert all(isinstance(caso, CasoCovidLanding) for caso in casos)
    
    # Verificar campos importantes
    for caso in casos[:10]:  # Verificar primeros 10 casos
        assert isinstance(caso.fecha_actualizacion, date)
        assert isinstance(caso.fecha_ingreso, date)
        assert isinstance(caso.fecha_sintomas, date)
        assert isinstance(caso.edad, int)
        assert isinstance(caso.id_registro, str)
        assert caso.source_file == "COVID19MEXICO.csv"
        
        # Verificar que las fechas no estén en el futuro
        hoy = datetime.now().date()
        assert caso.fecha_actualizacion <= hoy
        assert caso.fecha_ingreso <= hoy
        assert caso.fecha_sintomas <= hoy
        
        # Si hay fecha de defunción, debe ser válida
        if caso.fecha_def:
            assert isinstance(caso.fecha_def, date)
            assert caso.fecha_def <= hoy

def test_ingest_text_data(ingester):
    """Test ingestion of text data"""
    casos = ingester.ingest_text_data()
    assert len(casos) > 0
    assert all(isinstance(caso, TextoCasoLanding) for caso in casos)
    
    # Verificar que tenemos texto y keywords
    caso = casos[0]
    assert len(caso.texto_completo) > 0
    assert len(caso.keywords) > 0
    assert all(isinstance(k, str) for k in caso.keywords)
    assert isinstance(caso.fecha_extraccion, date)
