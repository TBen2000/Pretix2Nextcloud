import os

filename = "/test.xlsx"

verzeichnis, dateiname = os.path.split(filename)
verzeichnis = verzeichnis.strip("/\\")



print("Verzeichnis:", verzeichnis)
print("Dateiname:", dateiname)


path = "test/path/dir/"
print(os.path.join(path, verzeichnis, "Test"))


def upload_docker_image_version(filename: str = "Docker_Image_Version.txt", subdir: str = "") -> None:
    """
    Upload a Docker image version file indicating the Docker image currently used.
    """
    
    if not filename.lower().endswith(".txt"):
        filename += ".txt"
        
    print(filename)
    