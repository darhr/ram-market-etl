"""
Results table component module for the Streamlit app.

Provides the UI elements for displaying product results.
"""
import streamlit as st
from typing import List, Any

def render_results_table(products: List[Any]) -> None:
    """
    Renders a table displaying the list of products.

    Args:
        products (List[Any]): A list of product dictionaries or objects to display.
    """
    if not products:
        st.write("No se encontraron resultados.")
        return
    st.dataframe(products)
