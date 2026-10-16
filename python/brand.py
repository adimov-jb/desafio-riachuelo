"""Paleta da marca FinVarejo para gráficos.

Substitua os hex pelos oficiais do manual de marca; o resto do código usa só estes nomes.
Validada para daltonismo (CVD) com o validador de paleta: ΔE adjacente mínimo 12,4
(deuteranopia) e 25,4 (visão normal). O âmbar fica abaixo de 3:1 de contraste com o
fundo, por isso todo gráfico com a paleta precisa de rótulos visíveis por categoria.
"""

# Ordem categórica fixa: a cor segue a categoria, nunca a posição no ranking.
CATEGORICA = {
    "azul": "#1f4e9c",
    "coral": "#e4572e",
    "verde_agua": "#14a38b",
    "ambar": "#f0a202",
}

# Atribuição estável por canal de aquisição (mesma cor em qualquer gráfico/ordem).
CORES_CANAL = {
    "loja_fisica": CATEGORICA["azul"],
    "app": CATEGORICA["coral"],
    "site": CATEGORICA["verde_agua"],
    "televendas": CATEGORICA["ambar"],
}

SUPERFICIE = "#fcfcfb"
TEXTO_PRIMARIO = "#0b0b0b"
TEXTO_SECUNDARIO = "#52514e"
GRADE = "#e6e5e1"
