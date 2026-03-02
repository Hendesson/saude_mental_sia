import argparse
import os

import pandas as pd
import pyreadr

from config_paths import DATA_DIR, PROCESSED_DIR


def main() -> int:
    parser = argparse.ArgumentParser(description="Gera parquet agregado (mensal) a partir do RM15_SIA_Mental.RData.")
    parser.add_argument(
        "--input",
        default=os.path.join(DATA_DIR, "RM15_SIA_Mental.RData"),
        help="Caminho do .RData (default: data/RM15_SIA_Mental.RData)",
    )
    parser.add_argument(
        "--output",
        default=os.path.join(PROCESSED_DIR, "sia_mental_monthly.parquet"),
        help="Caminho do parquet (default: processed/sia_mental_monthly.parquet)",
    )
    args = parser.parse_args()

    if not os.path.exists(args.input):
        raise FileNotFoundError(f"Arquivo não encontrado: {args.input}")

    r = pyreadr.read_r(args.input)
    if not r:
        raise ValueError("Não foi possível ler o .RData (arquivo vazio ou corrompido).")

    if "RM15_SIA_Mental" in r:
        df = r["RM15_SIA_Mental"]
    else:
        df = next((obj for obj in r.values() if isinstance(obj, pd.DataFrame)), None)
        if df is None:
            raise ValueError("Nenhum data.frame encontrado dentro do .RData.")

    for col in ("pa_cmp", "RM_nome"):
        if col not in df.columns:
            raise ValueError(f"Coluna obrigatória ausente no dado: {col}")

    dff = df[["pa_cmp", "RM_nome"]].copy()
    dff["pa_cmp"] = dff["pa_cmp"].astype(str).str.strip()
    dff["ano"] = pd.to_numeric(dff["pa_cmp"].str.slice(0, 4), errors="coerce")
    dff["mes"] = pd.to_numeric(dff["pa_cmp"].str.slice(4, 6), errors="coerce")
    dff = dff.dropna(subset=["ano", "mes", "RM_nome"])
    dff["ano"] = dff["ano"].astype(int)
    dff["mes"] = dff["mes"].astype(int)
    dff["RM_nome"] = dff["RM_nome"].astype(str).str.strip()
    dff = dff[(dff["mes"] >= 1) & (dff["mes"] <= 12)]

    serie = (
        dff.groupby(["RM_nome", "ano", "mes"], as_index=False)
        .size()
        .rename(columns={"size": "casos_totais"})
    )
    serie["data"] = pd.to_datetime(dict(year=serie["ano"], month=serie["mes"], day=1), errors="coerce")
    serie = serie.dropna(subset=["data"]).sort_values(["RM_nome", "data"])

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    serie.to_parquet(args.output, index=False, engine="pyarrow")

    print(f"OK: gerado {args.output} com {len(serie)} linhas.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

