
def cargar_config():
    import configparser
    import os

    config = configparser.ConfigParser()
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, '..', 'config.ini')
    config.read(config_path)

    if not config.sections():
        raise FileNotFoundError(f"No se encontraron secciones en el archivo INI: {config_path}")

    return config
