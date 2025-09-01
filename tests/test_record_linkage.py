"""
Tests for COVID-19 record linkage functionality
"""
import pytest
import pandas as pd
import recordlinkage

class TestCovidRecordLinkage:
    @pytest.fixture
    def sample_data(self):
        """Generate two sample datasets for testing record linkage"""
        # Dataset A: from hospital system
        df_a = pd.DataFrame({
            'curp': ['ABCD123456', 'EFGH789012', 'IJKL345678'],
            'nombre': ['Juan Pérez', 'María García', 'Pedro López'],
            'edad': [45, 32, 58],
            'genero': ['M', 'F', 'M'],
            'telefono': ['5551234567', '5559876543', '5554567890']
        })
        
        # Dataset B: from another system with some variations
        df_b = pd.DataFrame({
            'curp': ['ABCD123456', 'EFGH789012', 'XYZW123456'],
            'nombre': ['Juan A Pérez', 'María García G', 'Ana Martínez'],
            'edad': [45, 33, 29],
            'genero': ['M', 'F', 'F'],
            'telefono': ['5551234567', '5559876544', '5551122334']
        })
        
        return df_a, df_b

    def test_basic_record_linkage(self, sample_data):
        """Test basic record linkage functionality"""
        df_a, df_b = sample_data
        
        # Initialize the indexer
        indexer = recordlinkage.Index()
        
        # Block on gender (more efficient than comparing all pairs)
        indexer.block('genero')
        candidate_pairs = indexer.index(df_a, df_b)
        
        # Create a comparison object
        compare = recordlinkage.Compare()
        
        # Define comparison methods
        compare.exact('curp', 'curp', label='curp_match')
        compare.string('nombre', 'nombre', method='jarowinkler', 
                      threshold=0.85, label='nombre_match')
        compare.numeric('edad', 'edad', label='edad_match', 
                       offset=1)  # Allow 1 year difference
        compare.exact('telefono', 'telefono', label='telefono_match')
        
        # Compute similarities
        features = compare.compute(candidate_pairs, df_a, df_b)
        
        # Convert to matches based on rules
        matches = features[features.sum(axis=1) >= 3]  # At least 3 matching criteria
        
        # Assertions
        assert len(matches) >= 1, "Should find at least one match"
        assert 'ABCD123456' in df_a.loc[matches.index.get_level_values(0), 'curp'].values, \
               "Should match Juan Pérez records"

    def test_fuzzy_name_matching(self, sample_data):
        """Test fuzzy name matching capabilities"""
        df_a, df_b = sample_data
        
        # Initialize the indexer
        indexer = recordlinkage.Index()
        indexer.full()  # Compare all pairs for this test
        candidate_pairs = indexer.index(df_a, df_b)
        
        # Create a comparison object focused on names
        compare = recordlinkage.Compare()
        compare.string('nombre', 'nombre', 
                      method='jarowinkler', 
                      threshold=0.85,
                      label='nombre_match')
        
        # Compute similarities
        features = compare.compute(candidate_pairs, df_a, df_b)
        
        # Get matches above threshold
        matches = features[features['nombre_match'] == 1]
        
        # Assertions
        assert len(matches) >= 2, "Should find at least two name matches"
        assert matches.index.get_level_values(0).nunique() >= 2, \
               "Should match multiple distinct records"

    def test_multi_criteria_matching(self, sample_data):
        """Test matching using multiple criteria with different weights"""
        df_a, df_b = sample_data
        
        indexer = recordlinkage.Index()
        indexer.block('genero')  # Block on gender first
        candidate_pairs = indexer.index(df_a, df_b)
        
        compare = recordlinkage.Compare()
        
        # Define comparisons with different methods
        compare.exact('curp', 'curp', label='curp_match')
        compare.string('nombre', 'nombre', 
                      method='jarowinkler', 
                      threshold=0.85,
                      label='nombre_match')
        compare.numeric('edad', 'edad', 
                       label='edad_match',
                       offset=1)
        compare.string('telefono', 'telefono', 
                      method='jarowinkler', 
                      threshold=0.9,
                      label='telefono_match')
        
        # Compute features
        features = compare.compute(candidate_pairs, df_a, df_b)
        
        # Apply weighted scoring
        weights = {
            'curp_match': 2.0,
            'nombre_match': 1.5,
            'edad_match': 1.0,
            'telefono_match': 1.0
        }
        
        weighted_scores = features.multiply(
            pd.Series(weights)
        ).sum(axis=1)
        
        # Consider matches above a threshold
        matches = weighted_scores[weighted_scores >= 4.0]
        
        # Assertions
        assert len(matches) > 0, "Should find matches with weighted criteria"
        # Check if high-confidence matches are found
        assert len(weighted_scores[weighted_scores >= 5.0]) > 0, \
               "Should find high-confidence matches"
