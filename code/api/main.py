from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from code.api.routes_data import router as data_router
from code.api.routes_uploads import router as upload_router
from code.api.scrna import router as scrna_router

WEB_DIR = Path(__file__).resolve().parents[1] / "web"

app = FastAPI(title="Olfactory Data API", version="0.1.0")
app.mount("/code/web", StaticFiles(directory=WEB_DIR, html=True), name="web")


@app.get("/")
def root():
    """Redirect root to the web dashboard."""
    return RedirectResponse(url="/code/web/index.html")


@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    ico = WEB_DIR / "favicon.ico"
    if ico.exists():
        return FileResponse(str(ico))
    raise HTTPException(status_code=404, detail="favicon not found")


# Wire routers
app.include_router(data_router)
app.include_router(upload_router)
app.include_router(scrna_router)
