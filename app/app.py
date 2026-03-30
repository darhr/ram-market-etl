"""
Main Streamlit application entry point.

Initializes the dashboard layout and components.
"""
import streamlit as st

def main():
    """
    Renders the Streamlit dashboard layout.
    """
    st.set_page_config(page_title="App de ejemplo", layout="wide")
    st.title("Ejemplo de dashboard con Streamlit")
    st.write("Texto de ejemplo")

if __name__ == "__main__":
    main()
