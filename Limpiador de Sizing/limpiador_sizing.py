import pandas as pd

def main():
    print("Limpiado de Sizing")

    root = r'C:\Users\cjuarez\Desktop\TAREAS PYTHON\Limpiador de Sizing\VentaMontoCategoria_115K.csv'

    plazas = [
        ["10DCU Humaya", "10DCU Culiacan Sur"],
        ["10DCU Culiacan", "10DCU Culiacan Norte"],
        ["10DCU Manzanillo", "10DCU Tepic Centro"],
        ["10GUD Chapala", "10GUD Tlajomulco"],
        ["10GUD Tlaquepaque", "10GUD GDL Poniente"],
        ["10GUD Tepic", "10GUD GDL Oriente"],
        ["10CAN Chichen Itza", "10CAN Merida"],
        ["10CAN Kukulkan", "10CAN Cancun"],
        ["10CAN Xcaret", "10CAN PlayaDelCarmen"],
        ["10CAN Uxmal", "10CAN Campeche"],
        ["10TLC Tol Zitacuaro", "10TLC Toluca Norte"],
        ["10TLC Toluca Nevado", "10TLC Toluca Sur"],
        ["10GTO Aguascalientes", "10GTO Ags Norte"],
        ["10GTO Guanajuato Ags", "10GTO Ags Sur"],
        ["10GTO Leon", "10GTO Leon Norte"],
        ["10GTO Guanajuato Sur", "10GTO Irapuato"],
        ["10MXL Sonora Norte", "10MXL San Luis RC"]
    ]

    diviciones = [
        ["OXX GUADALAJARA CHAPALA", "OXXO TLAJOMULCO"],
        ["OXX GUADALAJARA TLAQUEPAQUE", "OXXO GUADALAJARA PONIENTE"],
        ["OXX TEPIC", "OXXO GUADALAJARA ORIENTE"],
        ["Oxxo Humaya", "OXXO CULIACAN SUR"],
        ["Oxxo Culiacan", "OXXO CULIACAN NORTE"],
        ["Oxxo Manzanillo", "OXXO TEPIC CENTRO"],
        ["OXXO GUADALAJARA CHAPALA II", "OXXO TLAJOMULCO II"],
        ["OXXO MANZANILLO II", "OXXO TEPIC CENTRO II"],
        ["OXXO MANZANILLO", "OXXO TEPIC CENTRO"],
        ["OXXO HUMAYA", "OXXO CULIACAN SUR"],
        ["OXXO CULIACAN", "OXXO CULIACAN NORTE"]
    ]

    # Leer CSV
    #df = pd.read_csv(root)
    df = pd.read_csv('archivo.csv', encoding='utf-8-sig')



    # Convertir todas las columnas a texto para evitar errores
    df = df.astype(str)

    # ---- REEMPLAZOS DE PLAZAS ----
    for original, nuevo in plazas:
        df = df.replace(original, nuevo, regex=False)

    # ---- REEMPLAZOS DE DIVISIONES ----
    for original, nuevo in diviciones:
        df = df.replace(original, nuevo, regex=False)

    # Guardar archivo final
    output = root.replace(".csv", "_LIMPIO.csv")
    df.to_csv(output, index=False)

    print(f"Archivo generado: {output}")


if __name__ == "__main__":
    main()
