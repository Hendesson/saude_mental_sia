import logging
import os

import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, dcc, html

from data_processing import DataProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css",
    ],
)
server = app.server
app.title = "Dashboard de Saúde Mental - Análise"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _empty_figure(msg: str) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        template="plotly_white",
        annotations=[
            dict(
                text=msg,
                x=0.5,
                y=0.5,
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=16),
            )
        ],
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        height=520,
        margin=dict(l=20, r=20, t=60, b=20),
    )
    return fig


try:
    data = DataProcessor().load()
    df = data.series
    rms = data.rms
    anos = data.anos
    thresholds_by_rm = data.thresholds_by_rm
    logger.info("Dados carregados: %s linhas | %s RMs | %s anos", len(df), len(rms), len(anos))
except Exception as e:
    logger.error("Erro ao inicializar dados: %s", e)
    df = pd.DataFrame()
    rms = []
    anos = []
    thresholds_by_rm = {}
    data = None


cores_limiar = {
    "sem_risco": "#000099",
    "seguranca": "#009900",
    "baixo": "#FFD166",
    "moderado": "#ff8000",
    "alto": "#cc0000",
}


app.layout = dbc.Container(
    [
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.Img(src=app.get_asset_url("geocalor.png"), className="logo-img"),
                        html.H2(
                            "Dashboard de Saúde Mental - Análise",
                            className="text-center my-4",
                        ),
                        html.P(
                            "Série temporal mensal por Região Metropolitana (RM), com limiares por RM (quebras naturais).",
                            className="text-center subtitle-muted",
                        ),
                    ],
                    width=12,
                    className="text-center",
                )
            ]
        ),
        html.Br(),
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardHeader(html.H5("Filtros", className="mb-0 text-center")),
                            dbc.CardBody(
                                [
                                    dbc.Row(
                                        [
                                            dbc.Col(
                                                [
                                                    html.Div("RM (cidade/região)", className="control-label"),
                                                    dcc.Dropdown(
                                                        id="rm",
                                                        options=[{"label": r, "value": r} for r in rms],
                                                        value=rms[0] if rms else None,
                                                        clearable=False,
                                                    ),
                                                ],
                                                md=6,
                                            ),
                                            dbc.Col(
                                                [
                                                    html.Div("Intervalo de anos", className="control-label"),
                                                    dcc.RangeSlider(
                                                        id="anos",
                                                        min=min(anos) if anos else 2008,
                                                        max=max(anos) if anos else 2023,
                                                        step=1,
                                                        value=[min(anos), max(anos)] if anos else [2008, 2023],
                                                        marks=None,
                                                        tooltip={"placement": "bottom", "always_visible": True},
                                                        allowCross=False,
                                                    ),
                                                ],
                                                md=6,
                                            ),
                                        ],
                                        className="g-3",
                                    ),
                                    html.Hr(className="my-3"),
                                    dbc.Checklist(
                                        id="show-limiar",
                                        options=[{"label": "Mostrar limiares", "value": "on"}],
                                        value=["on"],
                                        inline=True,
                                        switch=True,
                                    ),
                                ]
                            ),
                        ]
                    ),
                    width=12,
                    className="mb-3",
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        [
                            dbc.CardHeader(html.H5("Série temporal", className="mb-0 text-center")),
                            dbc.CardBody(
                                dcc.Loading(dcc.Graph(id="serie-plot"), type="circle"),
                            ),
                        ]
                    ),
                    width=12,
                )
            ],
            className="mb-3",
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        dbc.Alert(
                            [
                                html.B("Dados: "),
                                html.Span("coloque `RM15_SIA_Mental.RData` em `SIA_MENTAL/data/` para habilitar o dashboard."),
                            ],
                            id="data-alert",
                            color="warning",
                            is_open=(data is None),
                            className="mt-3",
                        )
                    ],
                    width=12,
                )
            ]
        ),
    ],
    fluid=True,
)


@app.callback(
    Output("serie-plot", "figure"),
    Input("rm", "value"),
    Input("anos", "value"),
    Input("show-limiar", "value"),
)
def update_plot(rm, anos_range, show_limiar):
    if df.empty or not rm or not anos_range:
        return _empty_figure("Dados indisponíveis. Verifique `SIA_MENTAL/data/RM15_SIA_Mental.RData`.")

    y0, y1 = int(anos_range[0]), int(anos_range[1])
    dff = df[(df["RM_nome"] == rm) & (df["ano"] >= y0) & (df["ano"] <= y1)].copy()
    if dff.empty:
        return _empty_figure("Sem dados para os filtros selecionados.")

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=dff["data"],
            y=dff["casos_totais"],
            mode="lines+markers",
            name="Casos (mensal)",
            line=dict(color="#0066CC", width=2),
            marker=dict(size=5),
            hovertemplate="%{x|%b/%Y}<br>Casos: %{y}<extra></extra>",
        )
    )

    if "on" in (show_limiar or []):
        th = thresholds_by_rm.get(rm)
        if th:
            for key in ["sem_risco", "seguranca", "baixo", "moderado", "alto"]:
                fig.add_hline(
                    y=float(th[key]),
                    line_dash="dash",
                    line_width=1.5,
                    line_color=cores_limiar[key],
                    annotation_text=key.replace("_", " "),
                    annotation_position="top left",
                    opacity=0.9,
                )

    fig.update_layout(
        template="plotly_white",
        height=520,
        margin=dict(l=30, r=20, t=50, b=40),
        title=f"{rm} — Série temporal mensal",
        xaxis_title="Mês/Ano",
        yaxis_title="Número de casos",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    fig.update_xaxes(showgrid=True)
    fig.update_yaxes(showgrid=True)
    return fig


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=8050)

