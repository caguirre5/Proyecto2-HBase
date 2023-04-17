from databaseHandler import *
from inputHandler import *
from flet import *
import flet as ft


def main(page: ft.Page):
    def textbox_changed(e):
        textDisplay.value = Init(e.control.value)
        e.control.value = None
        page.update()

    textDisplay = ft.Text(size=24)
    textFielInput = ft.TextField(
        label="Ingrese un comando a ejecutar; recuerde respetar la sintaxis (ej. CREATE <table_name>, <column_family>):",
        text_size=24,
        on_submit=textbox_changed,
    )

    page.add(
        Column(
            controls=[
                Image(
                    src=f'https://hbase.apache.org/images/hbase_logo_with_orca_large.png',
                    fit=ft.ImageFit.CONTAIN,
                    width=1000,
                ),
                Container(
                    content=textFielInput,
                    margin=margin.symmetric(horizontal=100),
                ),
                textDisplay
            ],
            horizontal_alignment='center',
            expand=True,
        )
    )


ft.app(target=main, view=WEB_BROWSER)

