#!/usr/bin/env python3
"""
Student Pathways Dataset Creator
Creates a comprehensive table with students who graduated in 2023 and enrolled in higher education in 2024.
Each record contains student info, school info, and higher education enrollment details.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os

def convert_chilean_decimal(value):
    """Convert Chilean decimal format (comma) to float"""
    if pd.isna(value):
        return None
    try:
        return float(str(value).replace(',', '.'))
    except (ValueError, TypeError):
        return None

def load_and_clean_data():
    """Load and clean all required datasets"""
    print("📚 Loading datasets...")

    # Load School Directory Data
    print("  Loading school directory...")
    schools_df = pd.read_csv('data/20240912_Directorio_Oficial_EE_2024_20240430_WEB.csv', sep=';', encoding='utf-8')
    schools_df.columns = [col.replace('\ufeff', '') for col in schools_df.columns]
    print(f"    Schools loaded: {len(schools_df):,} records")

    # Load Student Graduation Data
    print("  Loading student graduation data...")
    students_df = pd.read_csv('data/20240606_NEM_PERCENTILES_JOVENES_2023_20240530_PUBL.csv', sep=';', encoding='utf-8')
    students_df.columns = [col.replace('\ufeff', '') for col in students_df.columns]

    # Convert NEM scores from Chilean format
    students_df['NEM_numeric'] = students_df['NEM'].apply(convert_chilean_decimal)
    print(f"    Students loaded: {len(students_df):,} records")

    # Load Higher Education Enrollment Data
    print("  Loading higher education enrollment data...")
    relevant_he_cols = ['mrun', 'gen_alu', 'tipo_inst_1', 'tipo_inst_2', 'cod_inst', 'nomb_inst',
                       'cod_sede', 'nomb_sede', 'cod_carrera', 'nomb_carrera', 'region_sede',
                       'comuna_sede', 'nivel_global', 'area_conocimiento', 'area_carrera_generica']

    he_df = pd.read_csv('data/20250729_Matrícula_Ed_Superior_2024_PUBL_MRUN.csv',
                       sep=';', encoding='utf-8', usecols=relevant_he_cols)
    he_df.columns = [col.replace('\ufeff', '') for col in he_df.columns]
    print(f"    Higher education enrollments loaded: {len(he_df):,} records")

    return schools_df, students_df, he_df

def create_pathways_dataset(schools_df, students_df, he_df):
    """Create the comprehensive pathways dataset"""
    print("\n🔄 Creating pathways dataset...")

    # Debug: Check available columns
    print("  Available columns in datasets:")
    print(f"    Schools: {list(schools_df.columns)[:10]}...")
    print(f"    Students: {list(students_df.columns)}")
    print(f"    Higher Ed: {list(he_df.columns)[:10]}...")
    print(f"    COD_DEPE in students data: {'COD_DEPE' in students_df.columns}")
    print(f"    COD_DEPE in schools data: {'COD_DEPE' in schools_df.columns}")

    # Find students with complete pathways (graduation → enrollment)
    graduated_students = set(students_df['MRUN'].unique())
    enrolled_students = set(he_df['mrun'].unique())
    pathway_students = graduated_students.intersection(enrolled_students)

    print(f"  Students graduated in 2023: {len(graduated_students):,}")
    print(f"  Students enrolled in higher ed 2024: {len(enrolled_students):,}")
    print(f"  Students with complete pathway: {len(pathway_students):,}")

    # Filter data to pathway students only
    pathway_graduates = students_df[students_df['MRUN'].isin(pathway_students)].copy()
    pathway_enrollments = he_df[he_df['mrun'].isin(pathway_students)].copy()

    print(f"  Pathway graduates: {len(pathway_graduates):,}")
    print(f"  Pathway enrollments: {len(pathway_enrollments):,}")

    # Check what school columns are actually available
    school_columns_needed = ['RBD', 'NOM_RBD', 'COD_REG_RBD', 'NOM_REG_RBD_A', 'COD_COM_RBD', 'NOM_COM_RBD', 'COD_DEPE', 'RURAL_RBD']
    school_columns_available = [col for col in school_columns_needed if col in schools_df.columns]
    school_columns_missing = [col for col in school_columns_needed if col not in schools_df.columns]

    print(f"  School columns available: {school_columns_available}")
    if school_columns_missing:
        print(f"  School columns missing: {school_columns_missing}")

    # Print all school columns to debug
    print(f"  All school columns: {list(schools_df.columns)}")

    # Merge graduation data with school information
    print("  Merging graduation data with school information...")
    grad_school_merged = pathway_graduates.merge(
        schools_df[school_columns_available],
        left_on='RBD',
        right_on='RBD',
        how='left'
    )

    print(f"  After school merge: {len(grad_school_merged):,} records")
    print(f"  Columns after school merge: {list(grad_school_merged.columns)}")

    # Check if COD_DEPE is available, if not try to get it from student data
    if 'COD_DEPE' not in grad_school_merged.columns and 'COD_DEPE' in pathway_graduates.columns:
        print("  COD_DEPE missing from school merge, using from student data...")
        # COD_DEPE is already in pathway_graduates, so it should be in the merged result

    # Merge with higher education enrollment data
    print("  Merging with higher education enrollment data...")
    final_dataset = grad_school_merged.merge(
        pathway_enrollments,
        left_on='MRUN',
        right_on='mrun',
        how='inner'
    )

    print(f"  Final dataset records: {len(final_dataset):,}")
    print(f"  Final dataset columns: {len(final_dataset.columns)}")
    print(f"  Final columns include COD_DEPE: {'COD_DEPE' in final_dataset.columns}")

    # Fix COD_DEPE column naming issue (merge creates COD_DEPE_x and COD_DEPE_y)
    if 'COD_DEPE_x' in final_dataset.columns:
        final_dataset['COD_DEPE'] = final_dataset['COD_DEPE_x']  # Use student data version
        print("  ✅ Fixed COD_DEPE column naming (using student data)")
    elif 'COD_DEPE_y' in final_dataset.columns:
        final_dataset['COD_DEPE'] = final_dataset['COD_DEPE_y']  # Use school data version
        print("  ✅ Fixed COD_DEPE column naming (using school data)")

    return final_dataset

def enhance_dataset(df):
    """Add calculated fields and clean up the dataset"""
    print("\n✨ Enhancing dataset...")
    print(f"  Starting with columns: {list(df.columns)}")

    # Keep numeric COD_DEPE for school type (more reliable)
    if 'COD_DEPE' in df.columns:
        print("  ✅ Raw COD_DEPE column available for school type analysis")
    else:
        print("  ⚠️  COD_DEPE not found")

    # Add rural/urban classification (if RURAL_RBD exists)
    if 'RURAL_RBD' in df.columns:
        df['location_type'] = df['RURAL_RBD'].map({0: 'Urban', 1: 'Rural'})
        print("  ✅ Added location type classification")
    else:
        df['location_type'] = 'Unknown'
        print("  ⚠️  RURAL_RBD not found, setting location type to Unknown")

    # Keep raw percentile for flexible analysis
    if 'PERCENTIL' in df.columns:
        print("  ✅ Raw PERCENTIL column available for analysis")
    else:
        print("  ⚠️  PERCENTIL not found")

    # Add NEM score categories (if NEM_numeric exists)
    if 'NEM_numeric' in df.columns:
        df['nem_category'] = pd.cut(
            df['NEM_numeric'],
            bins=[0, 5.0, 5.5, 6.0, 6.5, 7.0],
            labels=['< 5.0', '5.0-5.5', '5.5-6.0', '6.0-6.5', '6.5+'],
            include_lowest=True
        )
        print("  ✅ Added NEM score categories")
    else:
        df['nem_category'] = 'Unknown'
        print("  ⚠️  NEM_numeric not found, setting NEM category to Unknown")

    # Clean up gender field (if gen_alu exists)
    if 'gen_alu' in df.columns:
        gender_map = {1: 'Male', 2: 'Female'}
        df['gender_name'] = df['gen_alu'].map(gender_map)
        print("  ✅ Added gender descriptions")
    else:
        df['gender_name'] = 'Unknown'
        print("  ⚠️  gen_alu not found, setting gender to Unknown")

    print(f"  Enhanced dataset with {len(df.columns)} columns")

    return df

def select_final_columns(df):
    """Select and rename columns for the final dataset"""
    print("\n📋 Selecting final columns...")

    final_columns = {
        # Student Information
        'MRUN': 'student_id',
        'gender_name': 'student_gender',
        'AGNO_EGRESO': 'graduation_year',
        'NEM_numeric': 'nem_score',
        'PERCENTIL': 'percentile',
        'nem_category': 'nem_category',

        # School Information
        'RBD': 'school_id',
        'NOM_RBD': 'school_name',
        'COD_DEPE': 'school_type',
        'location_type': 'school_location_type',
        'COD_REG_RBD': 'school_region_code',
        'NOM_REG_RBD_A': 'school_region_name',
        'COD_COM_RBD': 'school_comuna_code',
        'NOM_COM_RBD': 'school_comuna_name',

        # Higher Education Information
        'cod_inst': 'institution_code',
        'nomb_inst': 'institution_name',
        'tipo_inst_1': 'institution_type',
        'tipo_inst_2': 'institution_subtype',
        'cod_sede': 'campus_code',
        'nomb_sede': 'campus_name',
        'region_sede': 'campus_region',
        'comuna_sede': 'campus_comuna',
        'cod_carrera': 'career_code',
        'nomb_carrera': 'career_name',
        'nivel_global': 'education_level',
        'area_conocimiento': 'knowledge_area',
        'area_carrera_generica': 'career_generic_area'
    }

    # Select only columns that exist in the dataframe
    available_columns = {k: v for k, v in final_columns.items() if k in df.columns}
    missing_columns = {k: v for k, v in final_columns.items() if k not in df.columns}

    print(f"  Available columns: {list(available_columns.keys())}")
    if missing_columns:
        print(f"  Missing columns: {list(missing_columns.keys())}")
        print(f"  Missing COD_DEPE? {'COD_DEPE' in missing_columns}")

    final_df = df[list(available_columns.keys())].copy()
    final_df = final_df.rename(columns=available_columns)

    print(f"  Final dataset columns: {len(final_df.columns)}")
    print(f"  school_type in final dataset: {'school_type' in final_df.columns}")

    return final_df

def save_dataset(df, output_path='student_pathways_2023_2024.csv'):
    """Save the dataset to CSV"""
    print(f"\n💾 Saving dataset to {output_path}...")

    df.to_csv(output_path, index=False, encoding='utf-8')

    print(f"  ✅ Dataset saved successfully!")
    print(f"  📊 Records: {len(df):,}")
    print(f"  📋 Columns: {len(df.columns)}")
    print(f"  📁 File size: {os.path.getsize(output_path) / (1024*1024):.1f} MB")

    return output_path

def generate_summary_stats(df):
    """Generate summary statistics for the dataset"""
    print("\n📊 Dataset Summary Statistics:")
    print("=" * 50)

    print(f"Total records: {len(df):,}")
    print(f"Unique students: {df['student_id'].nunique():,}")
    print(f"Unique schools: {df['school_id'].nunique():,}")
    print(f"Unique institutions: {df['institution_code'].nunique():,}")
    print(f"Unique careers: {df['career_code'].nunique():,}")

    print("\n📈 Distribution by School Type (COD_DEPE):")
    if 'school_type' in df.columns:
        school_type_dist = df['school_type'].value_counts().sort_index()
        school_type_names = {
            1: 'Municipal',
            2: 'Particular Subvencionado',
            3: 'Particular Pagado',
            4: 'Corporación Administración Delegada',
            5: 'Servicio Local de Educación',
            6: 'Otro'
        }
        for school_type, count in school_type_dist.items():
            percentage = (count / len(df)) * 100
            type_name = school_type_names.get(school_type, 'Unknown')
            print(f"  {school_type} ({type_name}): {count:,} ({percentage:.1f}%)")
    else:
        print("  School type data not available")

    print("\n🏛️ Distribution by Institution Type:")
    inst_type_dist = df['institution_type'].value_counts()
    for inst_type, count in inst_type_dist.items():
        percentage = (count / len(df)) * 100
        print(f"  {inst_type}: {count:,} ({percentage:.1f}%)")

    print("\n🎯 Percentile Distribution:")
    if 'percentile' in df.columns:
        print(f"  Mean percentile: {df['percentile'].mean():.1f}")
        print(f"  Median percentile: {df['percentile'].median():.1f}")
        print(f"  Students in top 10% (≤10): {(df['percentile'] <= 10).sum():,}")
        print(f"  Students in top 30% (≤30): {(df['percentile'] <= 30).sum():,}")
        print(f"  Students in bottom 30% (>70): {(df['percentile'] > 70).sum():,}")
    else:
        print("  Percentile data not available")

def main():
    """Main execution function"""
    print("🚀 Student Pathways Dataset Creator")
    print("=" * 50)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Load data
        schools_df, students_df, he_df = load_and_clean_data()

        # Create pathways dataset
        pathways_df = create_pathways_dataset(schools_df, students_df, he_df)

        # Enhance dataset
        enhanced_df = enhance_dataset(pathways_df)

        # Select final columns
        final_df = select_final_columns(enhanced_df)

        # Save dataset
        output_file = save_dataset(final_df)

        # Generate summary
        generate_summary_stats(final_df)

        print(f"\n✅ Process completed successfully!")
        print(f"📁 Output file: {output_file}")

        return final_df

    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise

if __name__ == "__main__":
    # Run the script
    dataset = main()

    # Show first few records
    print("\n👀 First 5 records:")
    print(dataset.head())

    print("\n📋 Column names:")
    for i, col in enumerate(dataset.columns, 1):
        print(f"  {i:2d}. {col}")