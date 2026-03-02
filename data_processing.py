import logging
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyreadr

from config_paths import DATA_DIR, PROCESSED_DIR

logger = logging.getLogger(__name__)


def _jenks_breaks(values: np.ndarray, n_classes: int) -> List[float]:
    """
    Jenks natural breaks (Fisher-Jenks) em Python puro.
    Retorna uma lista com (n_classes + 1) valores de quebra: [min, ..., max].

    Implementação baseada no algoritmo clássico de programação dinâmica.
    """
    x = np.asarray(values, dtype=float)
    x = x[np.isfinite(x)]
    if x.size == 0:
        return [0.0] * (n_classes + 1)
    if x.size == 1:
        return [float(x[0])] * (n_classes + 1)

    x.sort()
    n = x.size
    k = int(n_classes)
    k = max(2, k)

    # Matriz de limites (1-indexada no algoritmo)
    lower_class_limits = np.zeros((n + 1, k + 1), dtype=int)
    variance_combinations = np.full((n + 1, k + 1), np.inf, dtype=float)

    for i in range(1, k + 1):
        lower_class_limits[1, i] = 1
        variance_combinations[1, i] = 0.0

    for j in range(1, n + 1):
        lower_class_limits[j, 1] = 1
        variance_combinations[j, 1] = 0.0

    # DP
    for l in range(2, n + 1):
        s1 = 0.0
        s2 = 0.0
        w = 0.0

        for m in range(1, l + 1):
            i3 = l - m + 1
            val = x[i3 - 1]
            w += 1.0
            s1 += val
            s2 += val * val
            variance = s2 - (s1 * s1) / w

            if i3 > 1:
                for j in range(2, k + 1):
                    if variance_combinations[l, j] >= (variance + variance_combinations[i3 - 1, j - 1]):
                        lower_class_limits[l, j] = i3
                        variance_combinations[l, j] = variance + variance_combinations[i3 - 1, j - 1]

        lower_class_limits[l, 1] = 1
        variance_combinations[l, 1] = variance

    # Backtrack
    breaks = [0.0] * (k + 1)
    breaks[k] = float(x[-1])
    breaks[0] = float(x[0])

    count_num = k
    idx = n
    while count_num > 1:
        idxt = lower_class_limits[idx, count_num] - 1
        breaks[count_num - 1] = float(x[idxt])
        idx = lower_class_limits[idx, count_num] - 1
        count_num -= 1

    # Garante monotonicidade e tamanho
    for i in range(1, len(breaks)):
        if breaks[i] < breaks[i - 1]:
            breaks[i] = breaks[i - 1]
    return breaks


def _compute_thresholds(values: np.ndarray) -> Dict[str, float]:
    x = np.asarray(values, dtype=float)
    x = x[np.isfinite(x)]
    if x.size == 0:
        return {"sem_risco": 0.0, "seguranca": 0.0, "baixo": 0.0, "moderado": 0.0, "alto": 0.0}

    if x.size < 5:
        qs = np.quantile(x, [0.0, 0.2, 0.4, 0.6, 0.8, 1.0])
        brks = [float(v) for v in qs]
    else:
        brks = _jenks_breaks(x, n_classes=5)

    # Mantém o mesmo mapeamento do R: brks[1..5]
    return {
        "sem_risco": float(brks[0]),
        "seguranca": float(brks[1]),
        "baixo": float(brks[2]),
        "moderado": float(brks[3]),
        "alto": float(brks[4]),
    }


@dataclass(frozen=True)
class SIAData:
    series: pd.DataFrame
    rms: List[str]
    anos: List[int]
    thresholds_by_rm: Dict[str, Dict[str, float]]


class DataProcessor:
    def __init__(
        self,
        data_filename: str = "RM15_SIA_Mental.RData",
        processed_filename: str = "sia_mental_monthly.parquet",
    ):
        self.data_path = os.path.join(DATA_DIR, data_filename)
        self.processed_path = os.path.join(PROCESSED_DIR, processed_filename)

    def load(self) -> SIAData:
        # 1) Caminho preferencial (leve) para Render: parquet já agregado (pouquíssimas linhas)
        if os.path.exists(self.processed_path):
            serie = pd.read_parquet(self.processed_path, engine="pyarrow")
        else:
            # 2) Fallback: ler o .RData original (pode ser pesado). Ideal é pré-processar localmente.
            if not os.path.exists(self.data_path):
                raise FileNotFoundError(
                    "Dados não encontrados. Opções:\n"
                    f"- (recomendado) gere `{os.path.basename(self.processed_path)}` em `{PROCESSED_DIR}`\n"
                    f"- ou coloque `{os.path.basename(self.data_path)}` em `{DATA_DIR}`\n"
                )

            r = pyreadr.read_r(self.data_path)
            if not r:
                raise ValueError("Não foi possível ler o .RData (arquivo vazio ou corrompido).")

            # tenta pegar objeto esperado, senão o primeiro data.frame
            df: Optional[pd.DataFrame] = None
            if "RM15_SIA_Mental" in r:
                df = r["RM15_SIA_Mental"]
            else:
                for _, obj in r.items():
                    if isinstance(obj, pd.DataFrame):
                        df = obj
                        break

            if df is None or df.empty:
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
            serie["data"] = pd.to_datetime(
                dict(year=serie["ano"], month=serie["mes"], day=1),
                errors="coerce",
            )
            serie = serie.dropna(subset=["data"]).sort_values(["RM_nome", "data"])

            # tenta salvar o agregado para próximas execuções (se houver permissão)
            try:
                os.makedirs(PROCESSED_DIR, exist_ok=True)
                serie.to_parquet(self.processed_path, index=False, engine="pyarrow")
            except Exception as e:
                logger.warning("Não foi possível salvar parquet agregado: %s", e)

        # Normalização mínima (caso o parquet venha de outra fonte)
        required_cols = {"RM_nome", "ano", "mes", "casos_totais"}
        missing = required_cols - set(serie.columns)
        if missing:
            raise ValueError(f"Parquet/agregado inválido. Colunas faltando: {sorted(missing)}")

        if "data" not in serie.columns:
            serie["data"] = pd.to_datetime(dict(year=serie["ano"], month=serie["mes"], day=1), errors="coerce")
        serie = serie.dropna(subset=["data"]).sort_values(["RM_nome", "data"])

        rms = sorted(serie["RM_nome"].unique().tolist())
        anos = sorted(serie["ano"].unique().tolist())

        thresholds_by_rm: Dict[str, Dict[str, float]] = {}
        for rm, grp in serie.groupby("RM_nome"):
            thresholds_by_rm[rm] = _compute_thresholds(grp["casos_totais"].to_numpy())

        return SIAData(series=serie, rms=rms, anos=anos, thresholds_by_rm=thresholds_by_rm)

