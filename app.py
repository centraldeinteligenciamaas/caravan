"""
app.py
Servidor Flask que expõe endpoints de health check e sync manual,
e agenda a sincronização automática via APScheduler (7h e 17h BRT).

Deploy: Render Web Service  (Procfile define o start command)
Monitoramento: UptimeRobot → GET / a cada 5 min (mantém o serviço acordado)
"""

import os
import atexit
import logging
import threading
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

BRT = timezone(timedelta(hours=-3))

from caravan_supabase import main as run_sync

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

app = Flask(__name__)

_sync_lock = threading.Lock()
_last_run: dict = {"at": None, "status": "never"}


def _execute_sync():
    if not _sync_lock.acquire(blocking=False):
        logging.info("Sync já em andamento — execução ignorada.")
        return
    try:
        _last_run["at"] = datetime.now(BRT).strftime("%Y-%m-%dT%H:%M:%S BRT")
        _last_run["status"] = "running"
        logging.info("Iniciando sync...")
        run_sync()
        _last_run["status"] = "ok"
        logging.info("Sync concluído com sucesso.")
    except Exception as e:
        _last_run["status"] = f"error: {e}"
        logging.error("Sync falhou: %s", e)
    finally:
        _sync_lock.release()


@app.route("/")
def health():
    """UptimeRobot monitora este endpoint para manter o serviço ativo."""
    return jsonify({
        "status": "ok",
        "last_sync": _last_run,
    })


@app.route("/sync")
def trigger_sync():
    """Dispara a sincronização manualmente (execução assíncrona)."""
    if _sync_lock.locked():
        return jsonify({"status": "already_running"}), 409
    thread = threading.Thread(target=_execute_sync, daemon=True)
    thread.start()
    return jsonify({"status": "triggered"})


def _start_scheduler():
    """Inicia o APScheduler. Chamado uma única vez após o fork do gunicorn."""
    scheduler = BackgroundScheduler(timezone="America/Sao_Paulo")
    scheduler.add_job(_execute_sync, CronTrigger(hour=7,  minute=0), id="sync_manha")
    scheduler.add_job(_execute_sync, CronTrigger(hour=16, minute=0), id="sync_tarde")
    scheduler.start()
    atexit.register(scheduler.shutdown)
    logging.info("Scheduler iniciado — próximas execuções: 07:00 e 16:00 BRT")


# Inicia o scheduler no processo worker (gunicorn sem --preload importa o módulo
# somente no worker, então há exatamente 1 instância com --workers 1).
_start_scheduler()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
