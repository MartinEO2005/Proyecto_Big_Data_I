import os
import shutil


def save_output(df, output_path: str, header: bool = True, encoding: str = "utf-8"):
    """
    Guarda un DataFrame Spark:
    - en HDFS como carpeta Spark (part-00000..., _SUCCESS)
    - en local como CSV único si output_path no empieza por hdfs://
    """

    if output_path.startswith("hdfs://"):
        writer = (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", header)
        )

        if encoding:
            writer = writer.option("encoding", encoding)

        writer.csv(output_path)
        print(f"✅ CSV guardado en HDFS: {output_path}")
        return

    temp_dir = output_path + "_tmp"

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    if os.path.exists(output_path):
        if os.path.isdir(output_path):
            shutil.rmtree(output_path)
        else:
            os.remove(output_path)

    writer = (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", header)
    )

    if encoding:
        writer = writer.option("encoding", encoding)

    writer.csv(temp_dir)

    part_file = None
    for file_name in os.listdir(temp_dir):
        if file_name.startswith("part-") and file_name.endswith(".csv"):
            part_file = os.path.join(temp_dir, file_name)
            break

    if part_file is None:
        raise FileNotFoundError(f"No se encontró ningún part-*.csv en {temp_dir}")

    shutil.move(part_file, output_path)
    shutil.rmtree(temp_dir)

    print(f"✅ CSV guardado en local: {output_path}")
    