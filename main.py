#!/usr/bin/env python3
"""
Sistema principal OULAD - ETL y Análisis
"""

import sys
from pathlib import Path
from ETL.etl_process import ETLProcess

def check_datasets():
    """Verifica si los datasets están descargados."""
    datasets_path = Path("./Datasets")
    required_files = [
        "assessments.csv", "courses.csv", "studentAssessment.csv",
        "studentInfo.csv", "studentRegistration.csv", "studentVle.csv", "vle.csv"
    ]
    
    missing_files = []
    for file in required_files:
        if not (datasets_path / file).exists():
            missing_files.append(file)
    
    if missing_files:
        print("\n⚠️  Faltan los siguientes archivos de datos:")
        for file in missing_files:
            print(f"   - {file}")
        print("\nPor favor ejecuta primero:")
        print("  python Datasets/downloadDatasets.py Datasets/")
        return False
    
    return True

def run_etl():
    """Ejecuta el proceso ETL."""
    if not check_datasets():
        return
    
    print("\n¿Está seguro que desea ejecutar el ETL? Esto puede tomar varios minutos.")
    response = input("Continuar? (s/n): ")
    
    if response.lower() == 's':
        etl = ETLProcess()
        etl.run()
    else:
        print("ETL cancelado.")

def run_eda():
    """Ejecuta el análisis exploratorio de datos."""
    print("\n⚠️  EDA aún no implementado.")
    print("Esta funcionalidad estará disponible próximamente.")

def main():
    """Menú principal del sistema."""
    print("\n" + "="*50)
    print(" SISTEMA OULAD - ETL y Análisis de Datos")
    print("="*50)
    
    while True:
        print("\n=== MENÚ PRINCIPAL ===")
        print("1. Ejecutar ETL (Cargar datos a MySQL)")
        print("2. Ejecutar EDA (Análisis Exploratorio)")
        print("3. Verificar datasets")
        print("0. Salir")
        
        choice = input("\nSelecciona una opción: ")
        
        if choice == "1":
            run_etl()
        elif choice == "2":
            run_eda()
        elif choice == "3":
            if check_datasets():
                print("\n✓ Todos los datasets están disponibles.")
        elif choice == "0":
            print("\n¡Hasta luego!")
            break
        else:
            print("\n⚠️  Opción inválida. Por favor intenta de nuevo.")

if __name__ == "__main__":
    main()

