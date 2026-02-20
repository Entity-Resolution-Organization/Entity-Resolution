"""
Unit tests for bias_detection.py
Run: pytest tests/test_bias_detection.py -v
"""
import pytest
import pandas as pd
import numpy as np
import sys
import tempfile
import json
from pathlib import Path

sys.path.insert(0, './scripts')

from bias_detection import BiasDetector


@pytest.fixture
def detector():
    """Create BiasDetector with temp output directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield BiasDetector(output_dir=tmpdir)


@pytest.fixture
def sample_accounts():
    """Sample accounts dataframe."""
    return pd.DataFrame({
        'id': ['1', '2', '3', '4', '5'],
        'name': ['John Smith', 'Jane Doe', 'Bob Wilson', 'Alice Brown', 'Charlie Davis'],
        'address': ['123 Main St, Boston, MA', '456 Oak Ave, Austin, TX',
                   '789 Pine Rd, Seattle, WA', '321 Elm St, Denver, CO',
                   '654 Cedar Ln, Miami, FL']
    })


@pytest.fixture
def sample_pairs():
    """Sample pairs dataframe."""
    return pd.DataFrame({
        'id1': ['1', '2', '3', '4', '5'],
        'id2': ['1_var1', '2_var1', '6', '7', '8'],
        'label': [1, 1, 0, 0, 1]  # 3 positive, 2 negative
    })


class TestLanguageBias:
    """Test language bias detection."""

    def test_all_ascii_names_is_biased(self, detector):
        """All ASCII names should be flagged as language bias."""
        df = pd.DataFrame({
            'name': ['John Smith', 'Jane Doe', 'Bob Jones', 'Alice Brown']
        })

        result = detector.analyze_language_bias(df)

        assert result['non_ascii_percentage'] == 0.0
        assert result['has_language_bias'] == True
        assert result['severity'] == 'HIGH'

    def test_mixed_names_not_biased(self, detector):
        """Mixed character sets should not be flagged."""
        df = pd.DataFrame({
            'name': ['John Smith', '张伟', 'José García', 'محمد علي', 'Müller']
        })

        result = detector.analyze_language_bias(df)

        assert result['non_ascii_names'] == 4  # Chinese, Spanish, Arabic, German
        assert result['non_ascii_percentage'] == 80.0
        assert result['has_language_bias'] == False
        assert result['severity'] == 'LOW'

    def test_threshold_boundary(self, detector):
        """Test 5% threshold boundary."""
        # Create 100 names with exactly 5 non-ASCII
        names = ['John Smith'] * 95 + ['张伟', 'محمد', 'Müller', 'José', 'Николай']
        df = pd.DataFrame({'name': names})

        result = detector.analyze_language_bias(df)

        assert result['non_ascii_percentage'] == 5.0
        assert result['has_language_bias'] == False  # 5% is not < 5%

    def test_empty_names_handled(self, detector):
        """Empty/null names should be handled gracefully."""
        df = pd.DataFrame({
            'name': ['John', None, '', 'Jane', np.nan]
        })

        result = detector.analyze_language_bias(df)

        assert 'non_ascii_percentage' in result
        assert result['total_records'] == 5

    def test_missing_column_skipped(self, detector):
        """Missing name column should be skipped."""
        df = pd.DataFrame({'other_col': [1, 2, 3]})

        result = detector.analyze_language_bias(df)

        assert result['status'] == 'skipped'


class TestGeographicBias:
    """Test geographic bias detection."""

    def test_all_us_addresses_is_biased(self, detector):
        """100% US addresses should be flagged as geographic bias."""
        df = pd.DataFrame({
            'address': [
                '123 Main St, Boston, MA',
                '456 Oak Ave, Austin, TX',
                '789 Pine Rd, New York, NY',
                '321 Elm St, Los Angeles, CA'
            ]
        })

        result = detector.analyze_geographic_bias(df)

        assert result['us_percentage'] == 100.0
        assert result['has_geographic_bias'] == True
        assert result['severity'] in ['HIGH', 'MEDIUM']

    def test_international_addresses_not_biased(self, detector):
        """Mixed international addresses should not be flagged."""
        df = pd.DataFrame({
            'address': [
                '123 Main St, Boston, MA',
                '10 Downing St, London, UK',
                'Unter den Linden 1, Berlin, Germany',
                '1 Chome, Tokyo, Japan'
            ]
        })

        result = detector.analyze_geographic_bias(df)

        assert result['us_percentage'] < 80
        assert result['has_geographic_bias'] == False

    def test_threshold_80_percent(self, detector):
        """Test 80% threshold boundary."""
        # 4 US, 1 international = 80%
        df = pd.DataFrame({
            'address': [
                'Boston, MA', 'Austin, TX', 'Seattle, WA', 'Denver, CO',
                'London, UK'
            ]
        })

        result = detector.analyze_geographic_bias(df)

        # 80% is the boundary - should be flagged
        assert result['us_percentage'] == 80.0
        assert result['has_geographic_bias'] == False  # 80 is not > 80

    def test_empty_addresses_handled(self, detector):
        """Empty/null addresses should be handled."""
        df = pd.DataFrame({
            'address': ['Boston, MA', None, '', 'Austin, TX']
        })

        result = detector.analyze_geographic_bias(df)

        assert 'us_percentage' in result


class TestMatchLabelBias:
    """Test match label distribution bias."""

    def test_balanced_labels(self, detector):
        """50/50 split should not be flagged."""
        df = pd.DataFrame({
            'label': [1, 1, 1, 1, 1, 0, 0, 0, 0, 0]
        })

        result = detector.analyze_match_label_distribution(df)

        assert result['positive_percentage'] == 50.0
        assert result['negative_percentage'] == 50.0
        assert result['is_balanced'] == True
        assert result['has_label_bias'] == False

    def test_imbalanced_labels(self, detector):
        """Heavily imbalanced labels should be flagged."""
        df = pd.DataFrame({
            'label': [1, 1, 1, 1, 1, 1, 1, 1, 0, 0]  # 80% positive
        })

        result = detector.analyze_match_label_distribution(df)

        assert result['positive_percentage'] == 80.0
        assert result['is_balanced'] == False
        assert result['has_label_bias'] == True
        assert result['severity'] in ['HIGH', 'MEDIUM']

    def test_threshold_15_percent(self, detector):
        """Test 15% imbalance threshold."""
        # 65/35 split = 15% deviation from 50%
        df = pd.DataFrame({
            'label': [1] * 65 + [0] * 35
        })

        result = detector.analyze_match_label_distribution(df)

        assert result['imbalance_from_balanced'] == 15.0
        assert result['is_balanced'] == False  # 15 is not < 15

    def test_all_positive_pairs(self, detector):
        """100% positive pairs should be high severity."""
        df = pd.DataFrame({
            'label': [1, 1, 1, 1, 1]
        })

        result = detector.analyze_match_label_distribution(df)

        assert result['positive_percentage'] == 100.0
        assert result['has_label_bias'] == True
        assert result['severity'] == 'HIGH'

    def test_empty_pairs_skipped(self, detector):
        """Empty pairs dataframe should be skipped."""
        df = pd.DataFrame({'label': []})

        result = detector.analyze_match_label_distribution(df)

        assert result['status'] == 'skipped'


class TestDataSourceBias:
    """Test data source (synthetic vs real) bias detection."""

    def test_all_synthetic_is_biased(self, detector):
        """All synthetic IDs should be flagged."""
        df = pd.DataFrame({
            'id': ['1_var1', '1_var2', '2_var1', '3_synthetic', '4_test']
        })

        result = detector.analyze_data_source_bias(df)

        assert result['synthetic_percentage'] == 100.0
        assert result['has_source_bias'] == True

    def test_all_real_not_biased(self, detector):
        """All real IDs should not be flagged."""
        df = pd.DataFrame({
            'id': ['user_123', 'customer_456', 'record_789', 'entity_abc']
        })

        result = detector.analyze_data_source_bias(df)

        assert result['synthetic_percentage'] == 0.0
        assert result['has_source_bias'] == False

    def test_mixed_sources(self, detector):
        """Mixed real/synthetic should check threshold."""
        df = pd.DataFrame({
            'id': ['real_1', 'real_2', 'real_3', '1_var1']  # 25% synthetic
        })

        result = detector.analyze_data_source_bias(df)

        assert result['synthetic_percentage'] == 25.0
        assert result['has_source_bias'] == False


class TestEntityTypeBias:
    """Test entity type distribution bias."""

    def test_with_entity_type_column(self, detector):
        """Test with explicit entity_type column."""
        df = pd.DataFrame({
            'entity_type': ['person', 'person', 'company', 'product', 'person']
        })

        result = detector.analyze_entity_type_distribution(df)

        assert 'distribution' in result
        assert result['most_common'] == 'person'
        assert 'is_balanced' in result

    def test_inferred_from_names(self, detector):
        """Test entity type inference from name patterns."""
        df = pd.DataFrame({
            'name': ['John Smith', 'Acme Inc', 'Jane Doe', 'Tech Corp LLC']
        })

        result = detector.analyze_entity_type_distribution(df)

        assert result['status'] == 'inferred'
        assert 'distribution' in result


class TestFullBiasReport:
    """Test complete bias report generation."""

    def test_full_report_structure(self, sample_accounts, sample_pairs):
        """Test complete report has all required fields."""
        with tempfile.TemporaryDirectory() as tmpdir:
            detector = BiasDetector(output_dir=tmpdir)
            report = detector.generate_bias_report(sample_accounts, sample_pairs)

            assert 'timestamp' in report
            assert 'total_accounts' in report
            assert 'total_pairs' in report
            assert 'analyses' in report
            assert 'summary' in report

            # Check all analyses present
            analyses = report['analyses']
            assert 'language_bias' in analyses
            assert 'geographic_bias' in analyses
            assert 'data_source_bias' in analyses
            assert 'match_label_bias' in analyses

            # Check summary
            summary = report['summary']
            assert 'overall_bias_risk' in summary
            assert summary['overall_bias_risk'] in ['LOW', 'MEDIUM', 'HIGH']

    def test_report_saved_to_file(self, sample_accounts, sample_pairs):
        """Test report is saved to JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            detector = BiasDetector(output_dir=tmpdir)
            detector.generate_bias_report(sample_accounts, sample_pairs)

            report_path = Path(tmpdir) / 'bias_report.json'
            assert report_path.exists()

            with open(report_path) as f:
                saved_report = json.load(f)

            assert 'summary' in saved_report
            assert 'analyses' in saved_report

    def test_report_without_pairs(self, sample_accounts):
        """Test report generation without pairs data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            detector = BiasDetector(output_dir=tmpdir)
            report = detector.generate_bias_report(sample_accounts, pairs_df=None)

            assert report['total_pairs'] == 0
            assert 'match_label_bias' not in report['analyses']

    def test_overall_risk_calculation(self):
        """Test overall risk is calculated correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            detector = BiasDetector(output_dir=tmpdir)

            # Create high-bias scenario (all US, all ASCII)
            df = pd.DataFrame({
                'id': ['1', '2', '3'],
                'name': ['John', 'Jane', 'Bob'],
                'address': ['Boston, MA', 'Austin, TX', 'Denver, CO']
            })

            report = detector.generate_bias_report(df)

            # Should have HIGH risk due to language and geographic bias
            assert report['summary']['overall_bias_risk'] in ['HIGH', 'MEDIUM']
            assert report['summary']['total_issues'] >= 1


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_dataframe(self, detector):
        """Test with empty dataframe."""
        df = pd.DataFrame(columns=['id', 'name', 'address'])

        report = detector.generate_bias_report(df)

        assert report['total_accounts'] == 0
        assert 'summary' in report

    def test_single_record(self, detector):
        """Test with single record."""
        df = pd.DataFrame({
            'id': ['1'],
            'name': ['John Smith'],
            'address': ['Boston, MA']
        })

        report = detector.generate_bias_report(df)

        assert report['total_accounts'] == 1

    def test_special_characters_in_names(self, detector):
        """Test names with special characters."""
        df = pd.DataFrame({
            'name': ["O'Brien", 'Smith-Jones', 'José María', '李明']
        })

        result = detector.analyze_language_bias(df)

        assert 'non_ascii_percentage' in result
        # José María and 李明 have non-ASCII
        assert result['non_ascii_names'] >= 2


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
