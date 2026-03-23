# --- Streamlit custom style for full-width layout and better fit ---


import streamlit as st
import subprocess
import sys
import json
import os
import time
import threading
import queue
import re
import socket
import hashlib
import shutil
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse
import pandas as pd

# Try importing requests for API calls
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
 
st.markdown(
    """
    <style>
    /* Make main container full width */
    .main .block-container {
        max-width: 100000vw !important;
        padding-left: 2vw;
        padding-right: 2vw;
    }
    /*Make sidebar scrollable and fit */
    section[data-testid="stSidebar"] > div {
        max-width: 350px;
        min-width: 250px;
        overflow-y: auto;
    }
    /* Make tabs and expander content fit screen */
    .stTabs [data-baseweb="tab-list"] {
        flex-wrap: wrap;
    }
    .stExpanderContent {
        max-height: 60vh;
        overflow-y: auto;
    }
    /* Make dataframe and code blocks use more width */
    .stDataFrame, .stCodeBlock, .stMarkdown {
        max-width: 9800vw !important;
    }
    /* Responsive tweaks for mobile */
    @media (max-width: 1000px) {
        .main .block-container {
            padding-left: 0.5vw;
            padding-right: 0.5vw;
        }
        section[data-testid="stSidebar"] > div {
            max-width: 1000vw;
            min-width: 0;
        }
    }
    </style>
    """,
    unsafe_allow_html=True
)   
# Add this function after the other helper functions
def run_quick_extraction(url: str, extract_type: str):
    """Run quick extraction using site_extractor"""
    try:
        from site_extractor import quick_extract
        
        st.session_state.output_log = []
        st.session_state.output_log.append(f"🔍 Starting quick extraction for: {url}")
        st.session_state.output_log.append(f"📋 Extraction type: {extract_type}")
        
        # Run extraction
        result = quick_extract(url, extract_type)
        
        if result:
            st.session_state.results = pd.DataFrame([result])
            st.session_state.output_log.append("✅ Extraction completed successfully")
        else:
            st.session_state.output_log.append("❌ Extraction failed")
            
    except ImportError:
        st.session_state.output_log.append("❌ site_extractor module not found")
    except Exception as e:
        st.session_state.output_log.append(f"❌ Error: {str(e)}")

LLM_PROVIDERS = {
    "ollama": {
        "name": "🦙 Ollama (Local)",
        "description": "Local models via Ollama - No API key needed",
        "requires_key": False,
        "endpoint": "http://localhost:11434",
    },
    "openai": {
        "name": "🤖 OpenAI",
        "description": "GPT-4, GPT-3.5-turbo models",
        "requires_key": True,
        "models": ["gpt-4o", "gpt-4o-mini", "gpt-4-turbo", "gpt-4", "gpt-3.5-turbo"],
        "endpoint": "https://api.openai.com/v1",
    },
    "anthropic": {
        "name": "🧠 Anthropic",
        "description": "Claude 3.5 Sonnet, Claude 3 models",
        "requires_key": True,
        "models": ["claude-sonnet-4-20250514", "claude-3-5-sonnet-20241022", "claude-3-5-haiku-20241022", "claude-3-opus-20240229", "claude-3-haiku-20240307"],
        "endpoint": "https://api.anthropic.com/v1",
    },
    "gemini": {
        "name": "✨ Google Gemini",
        "description": "Gemini Pro, Gemini Flash models",
        "requires_key": True,
        "models": ["gemini-1.5-pro", "gemini-1.5-flash", "gemini-2.0-flash", "gemini-pro"],
        "endpoint": "https://generativelanguage.googleapis.com/v1beta",
    },
    "deepseek": {
        "name": "🌊 DeepSeek",
        "description": "DeepSeek-V3, DeepSeek Coder models",
        "requires_key": True,
        "models": ["deepseek-chat", "deepseek-coder", "deepseek-reasoner"],
        "endpoint": "https://api.deepseek.com/v1",
    },
}

def show_generated_spider_configs():
    config_dir = Path("configs")
    config_files = list(config_dir.glob("*_config.json"))
    config_names = [cfg_path.stem for cfg_path in config_files]

    st.sidebar.markdown("---")
    st.sidebar.markdown("### Generated Spiders")
    gen_spider_options = ["None"] + config_names if config_names else ["None"]
    selected_cfg = st.sidebar.selectbox("Select Generated Spider", gen_spider_options, key="gen_spider_dropdown")

    if 'last_selected_cfg' not in st.session_state:
        st.session_state.last_selected_cfg = None
    if selected_cfg != "None":
        st.session_state.last_selected_cfg = selected_cfg
    cfg_path = next((p for p in config_files if p.stem == st.session_state.last_selected_cfg), None) if config_files and st.session_state.last_selected_cfg else None

    if cfg_path:
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        st.markdown(f"### 🕷️ Spider Details: {cfg_path.stem}")
        st.write(f"**Name:** {cfg.get('name','')}")
        st.write(f"**Domain:** {cfg.get('domain','')}")
        st.write(f"**Description:** {cfg.get('description','')}")
        st.code(json.dumps(cfg, indent=2)[:2000], language="json")
        colA, colB = st.columns(2)
        with colA:
            if st.button(f"▶️ Run Spider: {cfg.get('name','')}", key=f"run_{cfg_path.stem}"):
                cmd = [sys.executable, "run.py", "--config", str(cfg_path)]
                st.info(f"Running: {' '.join(cmd)}")
                with st.spinner("Spider is running..."):
                    try:
                        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                        st.success("Spider finished successfully!")
                        st.code(result.stdout, language="text")
                    except subprocess.CalledProcessError as e:
                        st.error(f"Spider failed with error: {e}")

def check_ollama_running() -> bool:
    """Check if Ollama server is running"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('localhost', 11434))
        sock.close()
        return result == 0
    except Exception:
        return False


def check_mongo_running(mongo_uri: str = "mongodb://localhost:27017") -> bool:
    """Check if MongoDB is reachable."""
    try:
        from pymongo import MongoClient
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=2000)
        client.admin.command("ping")
        client.close()
        return True
    except Exception:
        return False


def start_mongo_from_ui(mongo_uri: str = "mongodb://localhost:27017") -> tuple[bool, str]:
    """Try to start MongoDB service/process from UI for local development."""
    if check_mongo_running(mongo_uri):
        return True, "MongoDB is already running"

    if os.name == "nt":
        for cmd in (["sc", "start", "MongoDB"], ["net", "start", "MongoDB"]):
            try:
                proc = subprocess.run(cmd, capture_output=True, text=True)
                output = (proc.stdout or "") + (proc.stderr or "")
                if proc.returncode == 0 or "already been started" in output.lower():
                    time.sleep(2)
                    if check_mongo_running(mongo_uri):
                        return True, "MongoDB service started"
            except Exception:
                pass

    mongod_path = shutil.which("mongod")
    if mongod_path:
        try:
            db_path = Path.home() / "data" / "db"
            db_path.mkdir(parents=True, exist_ok=True)
            subprocess.Popen(
                [mongod_path, "--dbpath", str(db_path)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            time.sleep(2)
            if check_mongo_running(mongo_uri):
                return True, f"MongoDB started with mongod (dbPath={db_path})"
        except Exception as exc:
            return False, f"Failed to start mongod: {exc}"

    return False, "Could not start MongoDB automatically. Start MongoDB service manually."

def validate_api_key(provider: str, api_key: str) -> tuple[bool, str]:
    """Validate API key for a provider (basic check)"""
    if not api_key or len(api_key) < 10:
        return False, "API key too short"
    
    if provider == "openai" and not api_key.startswith("sk-"):
        return False, "OpenAI keys should start with 'sk-'"
    
    if provider == "anthropic" and not api_key.startswith("sk-ant-"):
        return False, "Anthropic keys should start with 'sk-ant-'"
    
    return True, "Key format valid"

def extract_domain(url: str) -> str:
    """Extract domain name from URL"""
    try:
        if not url:
            return "unknown"
        parsed = urlparse(url.strip())
        domain = parsed.netloc or parsed.path.split('/')[0]
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain or "unknown"
    except:
        return "unknown"

def get_output_path(url: str, spider_key: str, output_format: str, 
                    auto_name: bool, organize: bool, new_file: bool) -> str:
    """Generate output file path based on settings"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Pipeline always writes .jsonl for json formats, .csv for csv
    file_ext = "jsonl" if output_format in ("json", "jsonl") else output_format
    # Folder is always "json" or "csv"
    folder_name = "json" if output_format in ("json", "jsonl") else output_format
    
    if auto_name and url:
        domain = extract_domain(url)
        if new_file:
            filename = f"{domain}_{timestamp}.{file_ext}"
        else:
            filename = f"{domain}.{file_ext}"
    else:
        if new_file:
            filename = f"output_{spider_key}_{timestamp}.{file_ext}"
        else:
            filename = f"output_{spider_key}.{file_ext}"
    
    if organize and url:
        domain = extract_domain(url)
        output_dir = Path("outputs") / domain / folder_name
        output_dir.mkdir(parents=True, exist_ok=True)
        return str(output_dir / filename)
    
    return filename

if 'output_log' not in st.session_state:
    st.session_state.output_log = []
if 'is_running' not in st.session_state:
    st.session_state.is_running = False
if 'results' not in st.session_state:
    st.session_state.results = None
if 'output_file' not in st.session_state:
    st.session_state.output_file = None
if 'run_started_at' not in st.session_state:
    st.session_state.run_started_at = None
if 'run_ended_at' not in st.session_state:
    st.session_state.run_ended_at = None

# LLM Configuration session state
if 'llm_enabled' not in st.session_state:
    st.session_state.llm_enabled = False
if 'llm_provider' not in st.session_state:
    st.session_state.llm_provider = "ollama"
if 'llm_model' not in st.session_state:
    st.session_state.llm_model = ""
if 'api_keys' not in st.session_state:
    st.session_state.api_keys = {
        "openai": "",
        "anthropic": "",
        "gemini": "",
        "deepseek": "",
    }

def detect_ollama_models():
    """Detect locally installed Ollama models"""
    try:
        import requests
        resp = requests.get("http://localhost:11434/api/tags", timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            models = [m["name"] for m in data.get("models", [])]
            return models
    except Exception:
        pass
    return ["llama3", "llama3.1", "mistral", "codellama"]

if 'ollama_models' not in st.session_state:
    st.session_state.ollama_models = detect_ollama_models()
if 'ollama_running' not in st.session_state:
    st.session_state.ollama_running = check_ollama_running()
if 'generated_config' not in st.session_state:
    st.session_state.generated_config = None
if 'mongo_enabled' not in st.session_state:
    st.session_state.mongo_enabled = os.getenv("MONGO_ENABLED", "true").lower() == "true"
if 'mongo_uri' not in st.session_state:
    st.session_state.mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
if 'mongo_database' not in st.session_state:
    st.session_state.mongo_database = os.getenv("MONGO_DATABASE", "geometric_crawler")
if 'mongo_collection' not in st.session_state:
    st.session_state.mongo_collection = os.getenv("MONGO_COLLECTION", "spider_items")
if 'mongo_running' not in st.session_state:
    st.session_state.mongo_running = check_mongo_running(st.session_state.mongo_uri)

# Spider configurations
SPIDERS = {
    "geometric": {
        "name": "🌐 Geometric Spider (Universal)",
        "description": "Universal spider with geometric pattern extraction for any website",
        "params": {
            "urls": {"type": "text", "label": "URLs (comma-separated)", "default": "", "required": True},
            "max_pages": {"type": "number", "label": "Max Pages", "default": 100, "min": 1, "max": 10000},
            "max_depth": {"type": "number", "label": "Max Depth", "default": 3, "min": 1, "max": 20},
            "http_only": {"type": "checkbox", "label": "HTTP Only (no Playwright)", "default": False},
            "use_http_after_first": {"type": "checkbox", "label": "HTTP after first layout", "default": True},
            "use_llm": {"type": "checkbox", "label": "Enable LLM Repair", "default": False},
            "cores": {"type": "number", "label": "Parallel Cores", "default": 4, "min": 1, "max": 16, 
                     "help": "Number of CPU cores to use for parallel extraction"},
            "concurrent_requests": {"type": "number", "label": "Concurrent Requests", "default": 16, "min": 1, "max": 100,
                                   "help": "Number of concurrent HTTP requests"},
            "use_fast_parser": {"type": "checkbox", "label": "Fast Parser (selectolax)", "default": True},
            "follow_patterns": {"type": "text", "label": "Follow URL Patterns (regex, comma-sep)", "default": ""},
        }
    },
    "drug": {
        "name": "💊 Drug Spider (Pharmaceutical)",
        "description": "Universal drug information spider with 40+ pharmacological categories",
        "params": {
            "start_url": {"type": "text", "label": "Start URL", "default": "", "required": True},
            "max_drugs": {"type": "number", "label": "Max Drugs (0 = unlimited)", "default": 0, "min": 0, "max": 100000},
            "max_subpage_depth": {"type": "number", "label": "Max Sub-page Depth", "default": 3, "min": 1, "max": 10},
            "max_subpages": {"type": "number", "label": "Max Sub-pages per Drug", "default": 20, "min": 1, "max": 100},
            "cores": {"type": "number", "label": "Parallel Cores", "default": 4, "min": 1, "max": 16,
                     "help": "Number of CPU cores to use for parallel extraction"},
            "concurrent_requests": {"type": "number", "label": "Concurrent Requests", "default": 8, "min": 1, "max": 50,
                                   "help": "Number of concurrent requests for faster extraction"},
            "follow_fda": {"type": "checkbox", "label": "Follow FDA Links", "default": True},
            "follow_links": {"type": "checkbox", "label": "Follow Links to Subpages", "default": True, "help": "Enable to crawl and extract from linked subpages (recommended)"},
            "is_listing": {"type": "select", "label": "URL Type", "default": "auto", "options": ["auto", "listing", "drug_page"], "help": "Force URL as listing page or direct drug page"},
            "use_fast_parser": {"type": "checkbox", "label": "Fast Parser (selectolax)", "default": True},
        }
    },
    "medlineplus": {
        "name": "🏥 MedlinePlus Spider (Medical)",
        "description": "Specialized spider for MedlinePlus drug information",
        "params": {
            "urls": {"type": "text", "label": "Override Start URLs (optional)", "default": ""},
            "max_pages": {"type": "number", "label": "Max Pages", "default": 100, "min": 1, "max": 5000},
            "max_depth": {"type": "number", "label": "Max Depth", "default": 2, "min": 1, "max": 10},
            "cores": {"type": "number", "label": "Parallel Cores", "default": 4, "min": 1, "max": 16,
                     "help": "Number of CPU cores to use for parallel extraction"},
            "concurrent_requests": {"type": "number", "label": "Concurrent Requests", "default": 4, "min": 1, "max": 20,
                                   "help": "Number of concurrent requests"},
            "use_existing_file": {"type": "checkbox", "label": "Resume from Existing File", "default": False},
            "resume_file": {"type": "text", "label": "Resume File Path (optional)", "default": ""},
            "follow_related": {"type": "checkbox", "label": "Follow Related Drug Links", "default": False},
            "use_fast_parser": {"type": "checkbox", "label": "Fast Parser (selectolax)", "default": True},
        }
    }
}

# Drug spider category mapping
DRUG_CATEGORIES = {
    "Basic Info": ["description", "uses", "brand_names", "generic_name", "drug_class"],
    "Dosage & Administration": ["dosage", "adult_dosage", "pediatric_dosage", "geriatric_dosage", 
                                  "renal_dosing", "hepatic_dosing", "route_of_administration"],
    "Pharmacokinetics (ADME)": ["pharmacokinetics", "absorption", "distribution", "metabolism", "elimination"],
    "Pharmacodynamics": ["pharmacodynamics", "mechanism_of_action", "receptor_binding", 
                         "onset_of_action", "duration_of_action", "clinical_pharmacology"],
    "Safety": ["side_effects", "common_side_effects", "serious_side_effects", "warnings", 
               "precautions", "contraindications", "allergic_reactions"],
    "Drug Interactions": ["drug_interactions", "food_interactions", "alcohol_interactions"],
    "Special Populations": ["pregnancy", "lactation", "fertility", "pediatric", "geriatric"],
    "Overdose": ["overdose", "overdose_symptoms", "overdose_treatment"],
    "Formulation": ["how_supplied", "ingredients", "storage"],
    "Regulatory": ["fda_info", "dea_schedule", "patient_info", "clinical_trials", "evidence"],
}

def build_command(spider_name: str, params: dict, output_file: str, output_format: str,
                  llm_config: dict = None) -> list:
    """Build scrapy command from parameters"""
    cmd = [sys.executable, "-m", "scrapy", "crawl", spider_name]

    # Mandatory: enforce selected parallel profile from UI (if present)
    profile = st.session_state.get("parallel_config", {})
    effective_cores = int(profile.get("cores", params.get("cores", 4)))
    effective_concurrent = int(profile.get("concurrent_requests", params.get("concurrent_requests", 16)))
    effective_delay = float(profile.get("download_delay", 0.25 if effective_cores > 4 else 0.5))
    effective_per_domain = int(profile.get("concurrent_requests_per_domain", max(1, effective_concurrent // 2)))
    
    # Parallel processing settings
    parallel_settings = {
        "geometric": ["cores", "concurrent_requests"],
        "drug": ["cores", "concurrent_requests"],
        "medlineplus": ["cores", "concurrent_requests"]
    }
    
    for key, value in params.items():
        if value is None or value == "":
            continue

        # Internal UI-only keys should never be passed to spider arguments.
        if str(key).startswith("__"):
            continue
        
        if key == "is_listing":
            if value == "auto":
                continue
            elif value == "listing":
                cmd.extend(["-a", "is_listing=true"])
            elif value == "drug_page":
                cmd.extend(["-a", "is_listing=false"])
            continue
        
        if isinstance(value, bool):
            cmd.extend(["-a", f"{key}={'true' if value else 'false'}"])
        else:
            cmd.extend(["-a", f"{key}={value}"])
    
    if llm_config and llm_config.get("enabled"):
        cmd.extend(["-a", "use_llm=true"])
        cmd.extend(["-a", f"llm_provider={llm_config.get('provider', 'ollama')}"])
        if llm_config.get("model"):
            cmd.extend(["-a", f"llm_model={llm_config['model']}"])
        if llm_config.get("api_key"):
            cmd.extend(["-a", f"llm_api_key={llm_config['api_key']}"])
    
    cmd.extend(["-a", f"output_format={output_format}"])
    # Pipeline handles output (CsvPipeline / JsonPipeline) - no -o flag needed
    cmd.extend(["-s", "LOG_LEVEL=INFO"])

    # Apply benchmark baseline for throttling and crawl stability.
    for setting_key, setting_value in BENCHMARK_CRAWL_SETTINGS.items():
        cmd.extend(["-s", f"{setting_key}={setting_value}"])

    # Resume support: persist scheduler queue and dupefilter state.
    jobdir = resolve_jobdir(spider_name, params)
    cmd.extend(["-s", f"JOBDIR={jobdir}"])
    
    # Apply parallel settings (mandatory profile -> benchmark defaults fallback).
    cmd.extend(["-s", f"CONCURRENT_REQUESTS={effective_concurrent}"])
    cmd.extend(["-s", f"CONCURRENT_REQUESTS_PER_DOMAIN={effective_per_domain}"])
    cmd.extend(["-s", f"DOWNLOAD_DELAY={effective_delay}"])
    cmd.extend(["-s", "RANDOMIZE_DOWNLOAD_DELAY=True"])
    cmd.extend(["-s", f"REACTOR_THREADPOOL_MAX_SIZE={max(20, effective_cores * 4)}"])

    # Memory settings for parallel processing
    cmd.extend(["-s", "MEMUSAGE_ENABLED=True"])
    cmd.extend(["-s", "MEMUSAGE_LIMIT_MB=1024"])
    cmd.extend(["-s", "MEMUSAGE_NOTIFY_MAIL=''"])
    
    return cmd


def summarize_effective_scrapy_settings(cmd: list[str]) -> dict:
    """Extract effective Scrapy settings from built command (last value wins)."""
    settings = {}
    i = 0
    while i < len(cmd):
        if cmd[i] == "-s" and i + 1 < len(cmd):
            pair = cmd[i + 1]
            if "=" in pair:
                key, value = pair.split("=", 1)
                settings[key] = value
            i += 2
            continue
        i += 1
    return settings


def build_resume_jobdir(spider_name: str, params: dict) -> str:
    """Create deterministic Scrapy JOBDIR path so queued requests can resume after stop/crash."""
    urls_value = str(
        params.get("urls")
        or params.get("start_url")
        or ""
    ).strip()
    fingerprint = hashlib.md5(urls_value.encode("utf-8")).hexdigest()[:12]
    state_dir = Path(".crawl_state")
    state_dir.mkdir(parents=True, exist_ok=True)
    return str(state_dir / f"{spider_name}_{fingerprint}")


def resolve_jobdir(spider_name: str, params: dict) -> str:
    """Resolve JOBDIR from UI (custom path) or deterministic default."""
    mode = str(st.session_state.get("jobdir_mode", "Auto (recommended)")).strip()
    custom = str(st.session_state.get("custom_jobdir", "")).strip()

    if mode.startswith("Custom") and custom:
        custom_path = Path(custom)
        custom_path.mkdir(parents=True, exist_ok=True)
        return str(custom_path)

    return build_resume_jobdir(spider_name, params)


def get_primary_start_url(params: dict) -> str:
    """Extract the primary start URL from spider params."""
    start_url = str(params.get("start_url", "")).strip()
    if start_url:
        return start_url
    urls_value = str(params.get("urls", "")).strip()
    if urls_value:
        return urls_value.split(",")[0].strip()
    return ""


def inspect_jobdir_resume_state(jobdir: str, spider_name: str, start_url: str) -> dict:
    """Inspect JOBDIR and prior context to determine if this run is a true resume."""
    jobdir_path = Path(jobdir)
    state = {
        "exists": jobdir_path.exists(),
        "has_state": False,
        "context_found": False,
        "url_match": False,
        "previous_url": "",
        "previous_spider": "",
    }

    if not jobdir_path.exists():
        return state

    has_requests_seen = (jobdir_path / "requests.seen").exists()
    has_queue = (jobdir_path / "requests.queue").exists()
    has_spider_state = (jobdir_path / "spider.state").exists()
    state["has_state"] = has_requests_seen or has_queue or has_spider_state

    context_file = jobdir_path / "resume_context.json"
    if context_file.exists():
        try:
            with open(context_file, "r", encoding="utf-8") as f:
                ctx = json.load(f)
            state["context_found"] = True
            state["previous_url"] = str(ctx.get("start_url", "")).strip()
            state["previous_spider"] = str(ctx.get("spider", "")).strip()
            state["url_match"] = (
                state["previous_spider"] == spider_name
                and state["previous_url"]
                and start_url
                and state["previous_url"] == start_url
            )
        except Exception:
            pass

    return state


def write_jobdir_resume_context(jobdir: str, spider_name: str, start_url: str):
    """Persist minimal run context so next run can confirm URL-level resume match."""
    try:
        jobdir_path = Path(jobdir)
        jobdir_path.mkdir(parents=True, exist_ok=True)
        context_file = jobdir_path / "resume_context.json"
        payload = {
            "spider": spider_name,
            "start_url": start_url,
            "updated_at": datetime.now().isoformat(),
        }
        with open(context_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        # Non-fatal: resume still works without this helper metadata.
        pass


def render_resume_status(spider_name: str, params: dict, effective: dict):
    """Show clear runtime message for resume behavior with same URL + JOBDIR."""
    jobdir = str(effective.get("JOBDIR", "")).strip()
    if not jobdir:
        return

    start_url = get_primary_start_url(params)
    state = inspect_jobdir_resume_state(jobdir, spider_name, start_url)

    if state["has_state"] and state["url_match"]:
        st.info(f"♻️ Running from existing URL queue: {state['previous_url']}")
    elif state["has_state"] and state["context_found"] and state["previous_url"]:
        st.warning(
            "JOBDIR has previous crawl state but URL differs. "
            f"Previous: {state['previous_url']} | Current: {start_url or 'n/a'}"
        )
    elif state["has_state"]:
        st.info(f"♻️ Running from existing JOBDIR queue: {jobdir}")
    else:
        st.caption(f"🆕 Starting fresh queue in JOBDIR: {jobdir}")

    write_jobdir_resume_context(jobdir, spider_name, start_url)


def write_persistent_run_log(log_lines: list[str], metrics: dict, exit_code: int | None, cmd: list[str]):
    """Always write a crawler log file with benchmark summary, even on non-zero exit."""
    log_dir = Path(".")
    log_file = log_dir / f"crawler_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    req = metrics.get("request_count")
    items = metrics.get("item_count")
    elapsed = metrics.get("elapsed_seconds", 0.0)
    avg_req = metrics.get("avg_req_per_sec")
    avg_items = metrics.get("avg_items_per_sec")

    req_text = str(req) if req is not None else "n/a"
    items_text = str(items) if items is not None else "n/a"
    avg_req_text = f"{avg_req:.2f}/s" if avg_req is not None else "n/a"
    avg_items_text = f"{avg_items:.2f}/s" if avg_items is not None else "n/a"

    header = [
        f"Timestamp: {datetime.now().isoformat()}",
        f"Exit code: {exit_code if exit_code is not None else 'unknown'}",
        f"Benchmark: requests_done={req_text}, items={items_text}, avg_req_speed={avg_req_text}, avg_extract_speed={avg_items_text}, elapsed={elapsed:.1f}s",
        f"Command: {' '.join(cmd)}",
        "=" * 80,
    ]

    with open(log_file, "w", encoding="utf-8") as f:
        for line in header:
            f.write(line + "\n")
        for line in log_lines:
            f.write(str(line) + "\n")

    return str(log_file)

# Update the geometric spider params to include more parallel options
SPIDERS = {
    "geometric": {
        "name": "🌐 Geometric Spider (Universal)",
        "description": "Universal spider with geometric pattern extraction for any website",
        "params": {
            "urls": {"type": "text", "label": "URLs (comma-separated)", "default": "", "required": True},
            "max_pages": {"type": "number", "label": "Max Pages", "default": 100, "min": 1, "max": 10000},
            "max_depth": {"type": "number", "label": "Max Depth", "default": 3, "min": 1, "max": 20},
            "http_only": {"type": "checkbox", "label": "HTTP Only (no Playwright)", "default": False},
            "use_http_after_first": {"type": "checkbox", "label": "HTTP after first layout", "default": True},
            "use_llm": {"type": "checkbox", "label": "Enable LLM Repair", "default": False},
            "cores": {"type": "number", "label": "Parallel Cores", "default": 4, "min": 1, "max": 16, 
                     "help": "Number of CPU cores to use for parallel extraction"},
            "concurrent_requests": {"type": "number", "label": "Concurrent Requests", "default": 16, "min": 1, "max": 100,
                                   "help": "Number of concurrent HTTP requests"},
            "use_fast_parser": {"type": "checkbox", "label": "Fast Parser (selectolax)", "default": True},
            "follow_patterns": {"type": "text", "label": "Follow URL Patterns (regex, comma-sep)", "default": ""},
        }
    },
    "drug": {
        "name": "💊 Drug Spider (Pharmaceutical)",
        "description": "Universal drug information spider with 40+ pharmacological categories",
        "params": {
            "start_url": {"type": "text", "label": "Start URL", "default": "", "required": True},
            "max_drugs": {"type": "number", "label": "Max Drugs (0 = unlimited)", "default": 0, "min": 0, "max": 100000},
            "max_subpage_depth": {"type": "number", "label": "Max Sub-page Depth", "default": 3, "min": 1, "max": 10},
            "max_subpages": {"type": "number", "label": "Max Sub-pages per Drug", "default": 20, "min": 1, "max": 100},
            "cores": {"type": "number", "label": "Parallel Cores", "default": 4, "min": 1, "max": 16,
                     "help": "Number of CPU cores to use for parallel extraction"},
            "concurrent_requests": {"type": "number", "label": "Concurrent Requests", "default": 8, "min": 1, "max": 50,
                                   "help": "Number of concurrent HTTP requests"},
            "follow_fda": {"type": "checkbox", "label": "Follow FDA Links", "default": True},
            "follow_links": {"type": "checkbox", "label": "Follow Links to Subpages", "default": True, "help": "Enable to crawl and extract from linked subpages (recommended)"},
            "is_listing": {"type": "select", "label": "URL Type", "default": "auto", "options": ["auto", "listing", "drug_page"], "help": "Force URL as listing page or direct drug page"},
            "use_fast_parser": {"type": "checkbox", "label": "Fast Parser (selectolax)", "default": True},
        }
    },
    "medlineplus": {
        "name": "🏥 MedlinePlus Spider (Medical)",
        "description": "Specialized spider for MedlinePlus drug information",
        "params": {
            "urls": {"type": "text", "label": "Override Start URLs (optional)", "default": ""},
            "max_pages": {"type": "number", "label": "Max Pages", "default": 100, "min": 1, "max": 5000},
            "max_depth": {"type": "number", "label": "Max Depth", "default": 2, "min": 1, "max": 10},
            "cores": {"type": "number", "label": "Parallel Cores", "default": 4, "min": 1, "max": 16,
                     "help": "Number of CPU cores to use for parallel extraction"},
            "concurrent_requests": {"type": "number", "label": "Concurrent Requests", "default": 4, "min": 1, "max": 20,
                                   "help": "Number of concurrent requests"},
            "use_existing_file": {"type": "checkbox", "label": "Resume from Existing File", "default": False},
            "resume_file": {"type": "text", "label": "Resume File Path (optional)", "default": ""},
            "follow_related": {"type": "checkbox", "label": "Follow Related Drug Links", "default": False},
            "use_fast_parser": {"type": "checkbox", "label": "Fast Parser (selectolax)", "default": True},
        }
    }
}

def run_spider(cmd: list, output_queue: queue.Queue, extra_env: dict = None):
    """Run spider in subprocess and capture output"""
    live_log_path = Path(".") / f"crawler_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    log_file = None
    started_monotonic = time.time()
    request_count = 0
    item_count = 0

    def _write_benchmark_snapshot(prefix: str = "Benchmark"):
        if not log_file:
            return
        elapsed = max(0.0, time.time() - started_monotonic)
        avg_req = (request_count / elapsed) if elapsed > 0 else 0.0
        avg_items = (item_count / elapsed) if elapsed > 0 else 0.0
        log_file.write(
            f"{prefix}: requests_done={request_count}, items={item_count}, "
            f"avg_req_speed={avg_req:.2f}/s, avg_extract_speed={avg_items:.2f}/s, "
            f"elapsed={elapsed:.1f}s\n"
        )
        log_file.flush()

    try:
        log_file = open(live_log_path, "w", encoding="utf-8")
        log_file.write(f"Timestamp: {datetime.now().isoformat()}\n")
        log_file.write("Exit code: running\n")
        log_file.write(f"Command: {' '.join(cmd)}\n")
        _write_benchmark_snapshot(prefix="Benchmark (startup)")
        log_file.write("=" * 80 + "\n")
        log_file.flush()

        output_queue.put(f" Live crawler log: {live_log_path}")

        env = os.environ.copy()
        if extra_env:
            env.update(extra_env)
        # Add parallel processing environment variables
        env.update({
            "SCRAPY_PROJECT": "geometric_crawler",
            "SCRAPY_SETTINGS_MODULE": "geometric_crawler.settings",
            "PYTHONUNBUFFERED": "1",  # Ensure real-time output
        })
        
        # Set multiprocessing start method for better performance
        if os.name != 'nt':  # Not on Windows
            env["PYTHONHASHSEED"] = "42"
            
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=str(Path(__file__).parent),
            env=env
        )
        
        for line in process.stdout:
            line_text = line.rstrip("\n")
            output_queue.put(line_text)

            # Track benchmark counters continuously from both periodic and final stats lines.
            m = re.search(r"Crawled\s+(\d+)\s+pages.*scraped\s+(\d+)\s+items", line_text)
            if m:
                request_count = max(request_count, int(m.group(1)))
                item_count = max(item_count, int(m.group(2)))
                _write_benchmark_snapshot(prefix="Benchmark (progress)")

            m_req = re.search(r"['\"]downloader/request_count['\"]\s*:\s*(\d+)", line_text)
            if m_req:
                request_count = max(request_count, int(m_req.group(1)))

            m_item = re.search(r"['\"]item_scraped_count['\"]\s*:\s*(\d+)", line_text)
            if m_item:
                item_count = max(item_count, int(m_item.group(1)))

            if log_file:
                log_file.write(line_text + "\n")
                log_file.flush()
            # Check for parallel processing messages
            if "Extracted in parallel" in line or "Using core" in line:
                # These are indicators of parallel extraction working
                pass
        process.wait()

        if log_file:
            log_file.write("=" * 80 + "\n")
            _write_benchmark_snapshot(prefix="Benchmark (final)")
            log_file.write(f"Exit code: {process.returncode}\n")
            log_file.write(f"Finished: {datetime.now().isoformat()}\n")
            log_file.flush()

        output_queue.put(f"__EXIT_CODE_{process.returncode}__")
    except Exception as e:
        if log_file:
            log_file.write("=" * 80 + "\n")
            _write_benchmark_snapshot(prefix="Benchmark (interrupted)")
            log_file.write(f"Runner error: {str(e)}\n")
            log_file.write(f"Finished: {datetime.now().isoformat()}\n")
            log_file.flush()
        output_queue.put(f"__ERROR__{str(e)}__")
    finally:
        if log_file:
            log_file.close()


# Baseline benchmark tuned for stable throughput and polite crawling.
BENCHMARK_CRAWL_SETTINGS = {
    "AUTOTHROTTLE_ENABLED": "True",
    "AUTOTHROTTLE_START_DELAY": "0.25",
    "AUTOTHROTTLE_MAX_DELAY": "3.0",
    "AUTOTHROTTLE_TARGET_CONCURRENCY": "8.0",
    "RANDOMIZE_DOWNLOAD_DELAY": "True",
    "RETRY_ENABLED": "True",
    "RETRY_TIMES": "3",
    "DOWNLOAD_TIMEOUT": "30",
}


def extract_runtime_metrics(log_lines: list[str], started_at: float, ended_at: float) -> dict:
    """Parse Scrapy logs to compute benchmark metrics: requests done and average speed."""
    metrics = {
        "request_count": None,
        "response_count": None,
        "item_count": None,
        "elapsed_seconds": max(0.0, ended_at - started_at),
        "avg_req_per_sec": None,
        "avg_items_per_sec": None,
    }

    request_patterns = [
        r"'downloader/request_count'\s*:\s*(\d+)",
        r'"downloader/request_count"\s*:\s*(\d+)',
    ]
    response_patterns = [
        r"'downloader/response_count'\s*:\s*(\d+)",
        r'"downloader/response_count"\s*:\s*(\d+)',
    ]
    item_patterns = [
        r"'item_scraped_count'\s*:\s*(\d+)",
        r'"item_scraped_count"\s*:\s*(\d+)',
    ]

    def _extract_last_int(patterns):
        value = None
        for line in log_lines:
            for pattern in patterns:
                match = re.search(pattern, line)
                if match:
                    value = int(match.group(1))
        return value

    metrics["request_count"] = _extract_last_int(request_patterns)
    metrics["response_count"] = _extract_last_int(response_patterns)
    metrics["item_count"] = _extract_last_int(item_patterns)

    # Fallback for interrupted runs where final stats block is missing.
    # Scrapy logstats lines often look like:
    # "Crawled 123 pages (at 45 pages/min), scraped 67 items (at 10 items/min)"
    crawled_fallback = None
    scraped_fallback = None
    page_rate_per_min = None
    item_rate_per_min = None
    for line in log_lines:
        crawled_match = re.search(r"Crawled\s+(\d+)\s+pages", line, re.IGNORECASE)
        if crawled_match:
            crawled_fallback = int(crawled_match.group(1))
        scraped_match = re.search(r"scraped\s+(\d+)\s+items", line, re.IGNORECASE)
        if scraped_match:
            scraped_fallback = int(scraped_match.group(1))
        page_rate_match = re.search(r"at\s+([\d.]+)\s+pages/min", line, re.IGNORECASE)
        if page_rate_match:
            page_rate_per_min = float(page_rate_match.group(1))
        item_rate_match = re.search(r"at\s+([\d.]+)\s+items/min", line, re.IGNORECASE)
        if item_rate_match:
            item_rate_per_min = float(item_rate_match.group(1))

    if metrics["request_count"] is None and crawled_fallback is not None:
        metrics["request_count"] = crawled_fallback
    if metrics["response_count"] is None and crawled_fallback is not None:
        metrics["response_count"] = crawled_fallback
    if metrics["item_count"] is None and scraped_fallback is not None:
        metrics["item_count"] = scraped_fallback

    elapsed = metrics["elapsed_seconds"]
    if elapsed > 0 and metrics["request_count"] is not None:
        metrics["avg_req_per_sec"] = metrics["request_count"] / elapsed
    if elapsed > 0 and metrics["item_count"] is not None:
        metrics["avg_items_per_sec"] = metrics["item_count"] / elapsed

    # If elapsed is not trustworthy on interrupted flow, use last reported rates.
    if metrics["avg_req_per_sec"] is None and page_rate_per_min is not None:
        metrics["avg_req_per_sec"] = page_rate_per_min / 60.0
    if metrics["avg_items_per_sec"] is None and item_rate_per_min is not None:
        metrics["avg_items_per_sec"] = item_rate_per_min / 60.0

    return metrics


def render_runtime_metrics(metrics: dict):
    """Render concise benchmark summary in UI."""
    req = metrics.get("request_count")
    items = metrics.get("item_count")
    elapsed = metrics.get("elapsed_seconds", 0.0)
    avg_req = metrics.get("avg_req_per_sec")
    avg_items = metrics.get("avg_items_per_sec")

    if req is None and items is None:
        st.caption("Benchmark metrics unavailable from logs for this run.")
        return

    req_text = str(req) if req is not None else "n/a"
    items_text = str(items) if items is not None else "n/a"
    avg_req_text = f"{avg_req:.2f}/s" if avg_req is not None else "n/a"
    avg_items_text = f"{avg_items:.2f}/s" if avg_items is not None else "n/a"
    st.info(
        f"Benchmark summary: requests done={req_text}, items={items_text}, "
        f"avg request speed={avg_req_text}, avg extraction speed={avg_items_text}, "
        f"elapsed={elapsed:.1f}s"
    )

def find_latest_output(directory: str, pattern: str = "*") -> str | None:
    """Find the most recently modified file in a directory matching pattern"""
    import glob
    if not os.path.exists(directory):
        return None
    files = glob.glob(os.path.join(directory, pattern))
    if not files:
        return None
    return max(files, key=os.path.getmtime)

def load_results(output_file: str) -> pd.DataFrame | None:
    """Load results from output file (supports JSON, JSONL, and CSV)"""
    if not os.path.exists(output_file):
        return None
    
    try:
        if output_file.endswith('.csv'):
            df = pd.read_csv(output_file)
            return df if not df.empty else None
        
        with open(output_file, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content or content == "[]":
                return None
        
        data = None
        try:
            data = json.loads(content)
            # Ensure data is always a list of items
            if isinstance(data, dict):
                data = [data]
            elif not isinstance(data, list):
                data = None
        except json.JSONDecodeError:
            data = []
            for line in content.split('\n'):
                line = line.strip()
                if line:
                    try:
                        obj = json.loads(line)
                        if isinstance(obj, dict):
                            data.append(obj)
                    except json.JSONDecodeError:
                        continue
        
        if not data:
            return None
        
        rows = []
        for item in data:
            row = {k: v for k, v in item.items() if k != 'data'}
            if 'data' in item and isinstance(item['data'], dict):
                for dk, dv in item['data'].items():
                    if isinstance(dv, str) and len(dv) > 500:
                        dv = dv[:500] + "..."
                    row[f"data_{dk}"] = dv
            rows.append(row)
        
        return pd.DataFrame(rows)
    except Exception as e:
        st.error(f"Error loading results: {e}")
        return None

# Sidebar
with st.sidebar:
    st.title("🕷️ Geometric Crawler")
    st.markdown("---")

    st.subheader("🍃 MongoDB Output")
    st.caption("Enable/disable MongoDB storage and manage local MongoDB service from UI")

    mongo_enabled = st.checkbox(
        "Enable MongoDB Storage",
        value=st.session_state.mongo_enabled,
        help="When enabled, scraped items are stored in MongoDB through MongoPipeline"
    )
    st.session_state.mongo_enabled = mongo_enabled

    st.session_state.mongo_uri = st.text_input(
        "Mongo URI",
        value=st.session_state.mongo_uri,
        help="Default local MongoDB URI"
    )
    st.session_state.mongo_database = st.text_input(
        "Mongo Database",
        value=st.session_state.mongo_database
    )
    st.session_state.mongo_collection = st.text_input(
        "Mongo Collection",
        value=st.session_state.mongo_collection
    )

    st.session_state.mongo_running = check_mongo_running(st.session_state.mongo_uri)
    if st.session_state.mongo_running:
        st.success("MongoDB is running")
    else:
        st.error("MongoDB is not reachable")

    mongo_col1, mongo_col2 = st.columns(2)
    with mongo_col1:
        if st.button("▶️ Start MongoDB", help="Try starting MongoDB service/process"):
            ok, msg = start_mongo_from_ui(st.session_state.mongo_uri)
            if ok:
                st.success(msg)
            else:
                st.error(msg)
            st.rerun()
    with mongo_col2:
        if st.button("🔄 Check Mongo", help="Re-check MongoDB connectivity"):
            st.rerun()

    with st.expander("Mongo Config Summary", expanded=False):
        st.json({
            "enabled": st.session_state.mongo_enabled,
            "uri": st.session_state.mongo_uri,
            "database": st.session_state.mongo_database,
            "collection": st.session_state.mongo_collection,
            "running": st.session_state.mongo_running,
        })

    st.markdown("---")
    
    spider_key = st.selectbox(
        "Select Spider",
        options=list(SPIDERS.keys()),
        format_func=lambda x: SPIDERS[x]["name"]
    )
    
    spider_config = SPIDERS[spider_key]
    st.info(spider_config["description"])
    
    st.markdown("---")
    
    st.subheader("📁 Output Settings")
    
    output_format = st.selectbox(
        "Output Format",
        options=["json", "jsonl", "csv"],
        index=0
    )
    
    if output_format == "csv":
        st.markdown("**📊 CSV Output Columns:**")
        st.caption("Select columns to include in CSV output")
        
        if spider_key == "medlineplus":
            available_columns = {
                "metadata": ["url", "domain", "scraped_at", "drug_id", "title"],
                "sections": ["uses", "dosage", "side_effects", "precautions", "warnings", 
                            "overdose", "missed_dose", "storage", "dietary_instructions",
                            "brand_names", "other_names", "other_uses", "other_info", 
                            "overview", "description"],
                "content": ["summary", "full_content"]
            }
        elif spider_key == "drug":
            available_columns = {
                "metadata": ["url", "domain", "scraped_at", "drug_name"],
                "sections": ["uses", "dosage", "side_effects", "precautions", "warnings",
                            "contraindications", "interactions", "overdose", "storage",
                            "brand_names", "generic_name", "drug_class"],
                "content": ["summary", "full_content"]
            }
        else:
            available_columns = {
                "metadata": ["url", "domain", "scraped_at", "title"],
                "sections": ["content", "description"],
                "content": ["full_content"]
            }
        
        col1, col2, col3 = st.columns(3)
        
        selected_columns = []
        with col1:
            st.markdown("**Metadata:**")
            for col in available_columns.get("metadata", []):
                if st.checkbox(col, value=True, key=f"csv_col_{col}"):
                    selected_columns.append(col)
        
        with col2:
            st.markdown("**Sections:**")
            for col in available_columns.get("sections", []):
                default_on = available_columns["sections"].index(col) < 5
                if st.checkbox(col, value=default_on, key=f"csv_col_{col}"):
                    selected_columns.append(col)
        
        with col3:
            st.markdown("**Content:**")
            for col in available_columns.get("content", []):
                if st.checkbox(col, value=True, key=f"csv_col_{col}"):
                    selected_columns.append(col)
        
        custom_csv_cols = st.text_input(
            "Additional Columns (comma-separated)",
            placeholder="custom_field1, custom_field2",
            key="custom_csv_columns"
        )
        if custom_csv_cols:
            selected_columns.extend([c.strip() for c in custom_csv_cols.split(",") if c.strip()])
        
        st.session_state.csv_output_columns = selected_columns
        
        with st.expander("📋 CSV Column Preview", expanded=False):
            header_preview = ",".join(selected_columns)
            st.code(header_preview, language="csv")
            st.info(f"📊 Total columns: **{len(selected_columns)}**")
    
    auto_name_by_domain = st.checkbox("Auto-name by Domain", value=True, 
                                       help="Automatically generate filename using the domain from URL")
    
    organize_by_domain = st.checkbox("Organize in Domain Folders", value=True,
                                      help="Save files in outputs/{domain}/{format}/ structure")
    
    new_file_always = st.checkbox("Always Create New File (with timestamp)", value=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_filename = f"output_{spider_key}_{timestamp}.{output_format}"
    
    output_filename = st.text_input("Output Filename (auto-updated from URL)", value=default_filename,
                                     help="Will be auto-generated if 'Auto-name by Domain' is checked")
    
    if organize_by_domain:
        st.caption(" Files saved to: `outputs/{domain}/{format}/`")

    st.markdown("---")
    st.subheader("♻️ Resume Queue (JOBDIR)")
    st.caption("Choose where Scrapy stores crawl queue/dupefilter state for pause/resume.")

    jobdir_mode_options = ["Auto (recommended)", "Custom path"]
    current_mode = st.session_state.get("jobdir_mode", "Auto (recommended)")
    if current_mode not in jobdir_mode_options:
        current_mode = "Auto (recommended)"

    st.session_state.jobdir_mode = st.selectbox(
        "JOBDIR Mode",
        options=jobdir_mode_options,
        index=jobdir_mode_options.index(current_mode),
        help="Auto uses .crawl_state/{spider}_{url-hash}. Custom lets you pick any folder.",
    )

    if st.session_state.jobdir_mode == "Custom path":
        st.session_state.custom_jobdir = st.text_input(
            "Custom JOBDIR path",
            value=st.session_state.get("custom_jobdir", ".crawl_state/custom"),
            help="Use the same path in future runs to resume from the same queue.",
        )
    else:
        st.caption("Resume Previous Queue (same URL): Auto mode reuses the prior crawl state for this spider+URL.")
        st.caption("Auto path is generated from spider + URL and resumes automatically when those match.")
    
    st.markdown("---")
    
    st.subheader(" LLM Repair Settings")
    
    llm_enabled = st.checkbox(
        "Enable LLM Repair",
        value=st.session_state.llm_enabled,
        help="Use LLM as last resort when other repair strategies fail"
    )
    st.session_state.llm_enabled = llm_enabled
    
    if llm_enabled:
        provider_options = list(LLM_PROVIDERS.keys())
        provider_labels = [LLM_PROVIDERS[p]["name"] for p in provider_options]
        
        selected_provider_idx = st.selectbox(
            "LLM Provider",
            options=range(len(provider_options)),
            format_func=lambda x: provider_labels[x],
            index=provider_options.index(st.session_state.llm_provider) if st.session_state.llm_provider in provider_options else 0
        )
        selected_provider = provider_options[selected_provider_idx]
        st.session_state.llm_provider = selected_provider
        
        provider_config = LLM_PROVIDERS[selected_provider]
        st.caption(provider_config["description"])
        
        if selected_provider == "ollama":
            ollama_running = check_ollama_running()
            st.session_state.ollama_running = ollama_running
            
            if ollama_running:
                st.success(" Ollama is running")
                
                col1, col2 = st.columns([3, 1])
                with col2:
                    if st.button("", help="Refresh models"):
                        st.session_state.ollama_models = detect_ollama_models()
                        st.rerun()
                
                ollama_models = st.session_state.ollama_models
                if ollama_models:
                    st.caption(f" {len(ollama_models)} model(s) installed")
                    
                    selected_model = st.selectbox(
                        "Select Model",
                        options=ollama_models,
                        index=ollama_models.index(st.session_state.llm_model) if st.session_state.llm_model in ollama_models else 0,
                        help="Choose from your locally installed Ollama models"
                    )
                    st.session_state.llm_model = selected_model
                else:
                    st.warning(" No models found")
                    st.caption("Install a model with:")
                    st.code("ollama pull llama3", language="bash")
                    
                    quick_col1, quick_col2 = st.columns(2)
                    with quick_col1:
                        if st.button(" llama3", help="Pull Llama 3 8B"):
                            st.session_state.show_ollama_pull = "llama3"
                    with quick_col2:
                        if st.button(" mistral", help="Pull Mistral 7B"):
                            st.session_state.show_ollama_pull = "mistral"
                    
                    if 'show_ollama_pull' in st.session_state and st.session_state.show_ollama_pull:
                        st.info(f"Run in terminal: `ollama pull {st.session_state.show_ollama_pull}`")
            else:
                st.error(" Ollama not running")
                
                st.markdown("**Start Ollama Server:**")
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    if st.button(" Start Ollama", type="primary", help="Start Ollama server in background"):
                        try:
                            import subprocess
                            if os.name == 'nt':
                                subprocess.Popen(
                                    ["ollama", "serve"],
                                    creationflags=subprocess.CREATE_NEW_CONSOLE,
                                    stdout=subprocess.DEVNULL,
                                    stderr=subprocess.DEVNULL
                                )
                            else:
                                subprocess.Popen(
                                    ["ollama", "serve"],
                                    stdout=subprocess.DEVNULL,
                                    stderr=subprocess.DEVNULL,
                                    start_new_session=True
                                )
                            st.info(" Starting Ollama... Please wait a few seconds and refresh")
                            time.sleep(2)
                            st.rerun()
                        except FileNotFoundError:
                            st.error(" Ollama not installed")
                            st.caption("Install from: https://ollama.ai")
                        except Exception as e:
                            st.error(f" Failed to start: {e}")
                
                with col2:
                    if st.button("🔄 Check", help="Check if Ollama started"):
                        st.rerun()
                
                st.caption("Or start manually: `ollama serve`")
                st.caption("Download: [ollama.ai](https://ollama.ai)")
        
        else:
            if provider_config["requires_key"]:
                api_key = st.text_input(
                    f"{provider_config['name'].split()[1]} API Key",
                    value=st.session_state.api_keys.get(selected_provider, ""),
                    type="password",
                    help=f"Enter your {provider_config['name'].split()[1]} API key"
                )
                st.session_state.api_keys[selected_provider] = api_key
                
                if api_key:
                    is_valid, msg = validate_api_key(selected_provider, api_key)
                    if is_valid:
                        st.success(f" {msg}")
                    else:
                        st.warning(f" {msg}")
                else:
                    st.caption(f"Get API key from {provider_config['name'].split()[1]} website")
                
                models = provider_config.get("models", [])
                if models:
                    selected_model = st.selectbox(
                        "Select Model",
                        options=models,
                        index=models.index(st.session_state.llm_model) if st.session_state.llm_model in models else 0
                    )
                    st.session_state.llm_model = selected_model
        
        with st.expander(" LLM Config Summary", expanded=False):
            st.json({
                "enabled": llm_enabled,
                "provider": selected_provider,
                "model": st.session_state.llm_model,
                "has_api_key": bool(st.session_state.api_keys.get(selected_provider)) if selected_provider != "ollama" else "N/A",
            })
    
    st.markdown("---")
    
    st.subheader(" Existing Outputs")
    outputs_dir = Path("outputs")
    if outputs_dir.exists():
        domains = [d.name for d in outputs_dir.iterdir() if d.is_dir()]
        if domains:
            selected_domain = st.selectbox("Browse Domain", options=["-- Select --"] + domains)
            if selected_domain != "-- Select --":
                domain_path = outputs_dir / selected_domain
                files = []
                for fmt_dir in domain_path.iterdir():
                    if fmt_dir.is_dir():
                        files.extend([(f.name, str(f)) for f in fmt_dir.glob("*")])
                if files:
                    st.caption(f"Found {len(files)} files")
                    load_file = st.selectbox("Load File", options=["-- Select --"] + [f[0] for f in files])
                    if load_file != "-- Select --":
                        file_path = next(f[1] for f in files if f[0] == load_file)
                        if st.button(" Load Selected"):
                            st.session_state.results = load_results(file_path)
                            st.session_state.output_file = file_path
                            st.success(f"Loaded: {load_file}")
                            st.rerun()
        else:
            st.caption("No outputs yet")
    else:
        st.caption("No outputs directory yet")
    
    st.markdown("---")
    
    st.subheader("⚡ Quick Presets")
    
    # Create tabs for different preset types
    preset_tab1, preset_tab2, preset_tab3 = st.tabs(["🕷️ Spider Presets", "🤖 LLM Extraction", "⚙️ Parallel Config"])
    
    with preset_tab1:
        if spider_key == "drug":
            preset = st.selectbox(
                "Load Spider Preset",
                options=["Custom", "RxList Example", "MedlinePlus Example", "High Volume"],
                index=0,
                key="spider_preset"
            )
            
            if preset == "RxList Example":
                st.session_state.preset_url = "https://www.rxlist.com/glucophage-drug.htm"
                st.success(" RxList preset loaded")
            elif preset == "MedlinePlus Example":
                st.session_state.preset_url = "https://medlineplus.gov/druginfo/meds/a682878.html"
                st.success(" MedlinePlus preset loaded")
            elif preset == "High Volume":
                st.session_state.preset_url = "https://www.rxlist.com/drugs/alpha_a.htm"
                st.success(" High Volume preset loaded")
    
    with preset_tab2:
        st.markdown("### 🧬 Site Extractor")
        st.caption("Extract structured data from any website using LLM")
        
        # Check if LLM is enabled
        if not st.session_state.get('llm_enabled', False):
            st.warning("⚠️ LLM must be enabled above to use extraction")
        else:
            # Quick URL input for extraction
            extract_url = st.text_input(
                "URL to Extract",
                placeholder="https://example.com/drug-page",
                key="quick_extract_url",
                help="Enter a URL to analyze and extract structured data"
            )
            
            # Extraction type selection
            extract_type = st.radio(
                "Extraction Type",
                options=["Auto-Detect", "Drug Info", "Product Data", "Article", "Custom"],
                horizontal=True,
                key="extract_type"
            )
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🔍 Analyze Page", key="quick_analyze_btn", use_container_width=True):
                    if extract_url:
                        st.session_state.analyze_url = extract_url
                        st.session_state.show_extractor = True
                        st.info(f"✅ Analyzing: {extract_url}")
                        # Set the URL in the main extractor tab
                        st.session_state.gen_master_url = extract_url
                        st.session_state.gen_list_url = extract_url
                        st.session_state.gen_detail_url = extract_url
                        st.rerun()
                    else:
                        st.error("Please enter a URL")
            
            with col2:
                if st.button("⚡ Quick Extract", key="quick_extract_btn", use_container_width=True, type="primary"):
                    if extract_url:
                        st.session_state.quick_extract_url = extract_url
                        st.session_state.quick_extract_type = extract_type
                        st.session_state.run_quick_extract = True
                        st.rerun()
                    else:
                        st.error("Please enter a URL")
            
            # Common site presets
            st.markdown("#### 📋 Site Presets")
            site_preset = st.selectbox(
                "Common Sites",
                options=["Custom", "RxList Drug Page", "MedlinePlus", "Drugs.com", "Mayo Clinic", "Wikipedia Drug"],
                key="site_preset"
            )
            
            if site_preset != "Custom":
                preset_urls = {
                    "RxList Drug Page": "https://www.rxlist.com/glucophage-drug.htm",
                    "MedlinePlus": "https://medlineplus.gov/druginfo/meds/a682878.html",
                    "Drugs.com": "https://www.drugs.com/glucophage.html",
                    "Mayo Clinic": "https://www.mayoclinic.org/drugs-supplements/",
                    "Wikipedia Drug": "https://en.wikipedia.org/wiki/Metformin"
                }
                if st.button(f"📥 Load {site_preset}", key=f"load_{site_preset}"):
                    st.session_state.quick_extract_url = preset_urls[site_preset]
                    st.rerun()
    
    with preset_tab3:
        st.markdown("### ⚙️ Parallel Extraction Settings")
        st.caption("Configure parallel processing for faster extraction")
        
        # Parallel configuration presets
        parallel_preset = st.selectbox(
            "Performance Profile",
            options=["Balanced (Default)", "Fast (High Concurrency)", "Gentle (Low Impact)", "Custom"],
            key="parallel_preset"
        )
        
        if parallel_preset == "Balanced (Default)":
            cores = 4
            concurrent = 16
            delay = 0.25
            per_domain = 8
        elif parallel_preset == "Fast (High Concurrency)":
            cores = 8
            concurrent = 32
            delay = 0.1
            per_domain = 16
        elif parallel_preset == "Gentle (Low Impact)":
            cores = 2
            concurrent = 4
            delay = 1.0
            per_domain = 2
        else:
            cores = st.slider("CPU Cores", 1, 16, 4)
            concurrent = st.slider("Concurrent Requests", 1, 50, 16)
            delay = st.slider("Download Delay (s)", 0.0, 2.0, 0.25, 0.05)
            per_domain = st.slider("Per Domain Concurrency", 1, 25, 8)
        
        # Store parallel config in session state
        st.session_state.parallel_config = {
            "cores": cores,
            "concurrent_requests": concurrent,
            "download_delay": delay,
            "concurrent_requests_per_domain": per_domain,
        }
        
        # Show current config
        with st.expander("📊 Current Parallel Config", expanded=False):
            st.json(st.session_state.parallel_config)
        
        # Apply to panel fields used by all spiders (each spider clamps to its own limits).
        if st.button("⚡ Apply to All Spiders", key="apply_parallel"):
            st.session_state["param_cores"] = int(cores)
            st.session_state["param_concurrent_requests"] = int(concurrent)
            st.success(f"✅ Parallel config applied globally: {concurrent} concurrent requests, {cores} cores")
            st.rerun()
    
    st.markdown("---")
    
    # Add a quick extractor status indicator
    if st.session_state.get("show_extractor", False):
        st.info("🔍 Extractor active in Spider Generator tab")
        
# Main content
st.title(spider_config["name"])

# Parameter tabs
tab1, tab2, tab3, tab4 = st.tabs(["⚙️ Configuration", "📊 Results", "📋 Log", "🧬 Spider Generator"])

# Handle quick extraction if triggered
if st.session_state.get("run_quick_extract", False):
    url = st.session_state.quick_extract_url
    extract_type = st.session_state.quick_extract_type
    
    with st.spinner(f"Extracting data from {url}..."):
        run_quick_extraction(url, extract_type)
    
    # Clear the flag
    st.session_state.run_quick_extract = False
    st.session_state.show_extractor = True
    st.rerun()
    
with tab1:
    st.subheader("Spider Parameters")
    
    params = {}

    col1, col2 = st.columns(2)

    param_items = list(spider_config["params"].items())
    half = len(param_items) // 2 + len(param_items) % 2

    # Always show URL Type dropdown for Drug Spider
    if spider_key == "drug":
        st.markdown("### URL Type (Listing or Drug Page)")
        url_type_options = ["auto", "listing", "drug_page"]
        url_type_default = "auto"
        params["is_listing"] = st.selectbox(
            "URL Type",
            options=url_type_options,
            index=url_type_options.index(url_type_default),
            help="Force URL as listing page or direct drug page"
        )

    with col1:
        for key, config in param_items[:half]:
            if key == "is_listing" and spider_key == "drug":
                continue  # Already rendered above
            help_text = config.get("help", None)
            if config["type"] == "text":
                default = st.session_state.get('preset_url', config["default"]) if key in ['start_url', 'urls'] else config["default"]
                params[key] = st.text_input(
                    config["label"],
                    value=default,
                    key=f"param_{key}",
                    help=help_text
                )
            elif config["type"] == "number":
                number_default = config["default"]
                if key in {"cores", "concurrent_requests"} and "parallel_config" in st.session_state:
                    number_default = st.session_state.parallel_config.get(key, number_default)
                min_v = config.get("min", 1)
                max_v = config.get("max", 10000)
                state_key = f"param_{key}"
                if state_key in st.session_state:
                    st.session_state[state_key] = max(min_v, min(max_v, st.session_state[state_key]))
                    params[key] = st.number_input(
                        config["label"],
                        min_value=min_v,
                        max_value=max_v,
                        key=state_key,
                        help=help_text
                    )
                else:
                    params[key] = st.number_input(
                        config["label"],
                        value=max(min_v, min(max_v, number_default)),
                        min_value=min_v,
                        max_value=max_v,
                        key=state_key,
                        help=help_text
                    )
            elif config["type"] == "checkbox":
                params[key] = st.checkbox(
                    config["label"],
                    value=config["default"],
                    key=f"param_{key}",
                    help=help_text
                )
            elif config["type"] == "select":
                options = config.get("options", [])
                default_idx = options.index(config["default"]) if config["default"] in options else 0
                params[key] = st.selectbox(
                    config["label"],
                    options=options,
                    index=default_idx,
                    key=f"param_{key}",
                    help=help_text
                )

    with col2:
        for key, config in param_items[half:]:
            if key == "is_listing" and spider_key == "drug":
                continue  # Already rendered above
            help_text = config.get("help", None)
            if config["type"] == "text":
                params[key] = st.text_input(
                    config["label"],
                    value=config["default"],
                    key=f"param_{key}",
                    help=help_text
                )
            elif config["type"] == "number":
                number_default = config["default"]
                if key in {"cores", "concurrent_requests"} and "parallel_config" in st.session_state:
                    number_default = st.session_state.parallel_config.get(key, number_default)
                min_v = config.get("min", 1)
                max_v = config.get("max", 10000)
                state_key = f"param_{key}"
                if state_key in st.session_state:
                    st.session_state[state_key] = max(min_v, min(max_v, st.session_state[state_key]))
                    params[key] = st.number_input(
                        config["label"],
                        min_value=min_v,
                        max_value=max_v,
                        key=state_key,
                        help=help_text
                    )
                else:
                    params[key] = st.number_input(
                        config["label"],
                        value=max(min_v, min(max_v, number_default)),
                        min_value=min_v,
                        max_value=max_v,
                        key=state_key,
                        help=help_text
                    )
            elif config["type"] == "checkbox":
                params[key] = st.checkbox(
                    config["label"],
                    value=config["default"],
                    key=f"param_{key}",
                    help=help_text
                )
            elif config["type"] == "select":
                options = config.get("options", [])
                default_idx = options.index(config["default"]) if config["default"] in options else 0
                params[key] = st.selectbox(
                    config["label"],
                    options=options,
                    index=default_idx,
                    key=f"param_{key}",
                    help=help_text
                )
    
    if spider_key == "drug":
        st.markdown("---")
        st.subheader("🔗 Link Pattern Guide")
        with st.expander("How to configure for ANY website", expanded=False):
            st.markdown("""
**Link Pattern** - Regex to match drug/product page URLs:
| Site | Pattern |
|------|---------|
| Netmeds | `/prescriptions/` |
| 1mg | `/drugs/` |
| Pharmeasy | `/product/` |
| RxList | `-drug\\.htm` |

**Exclude Pattern** - Regex to skip category/listing pages:
| Site | Pattern |
|------|---------|
| Netmeds | `/collection/\\|/sections/` |
| 1mg | `/categories/` |
| Generic | `/browse/\\|/search\\?` |

**URL Type**:
- `auto` - Spider auto-detects if URL is listing or drug page
- `listing` - Force URL as listing page (will search for drug links)
- `drug_page` - Force URL as direct drug page (will extract content)
            """)
        
        st.markdown("---")
        st.subheader("🏷️ Category Mapping")
        st.info("These categories define which sections are extracted from drug pages")
        
        with st.expander("View/Edit Extraction Categories", expanded=False):
            for category, fields in DRUG_CATEGORIES.items():
                st.markdown(f"**{category}:**")
                st.text(", ".join(fields))
    
    st.markdown("---")
    st.subheader("📁 Output Preview")
    
    preview_url = params.get('start_url', '') or params.get('urls', '').split(',')[0].strip()
    if preview_url:
        preview_path = get_output_path(
            url=preview_url,
            spider_key=spider_key,
            output_format=output_format,
            auto_name=auto_name_by_domain,
            organize=organize_by_domain,
            new_file=new_file_always
        )
        st.success(f"📄 Output will be saved to: `{preview_path}`")
        
        domain = extract_domain(preview_url)
        st.caption(f"Domain detected: **{domain}**")
    else:
        st.warning("Enter a URL to see output path preview")
    
    st.markdown("---")
    
    llm_config = {
        "enabled": llm_enabled,
        "provider": st.session_state.llm_provider,
        "model": st.session_state.llm_model,
        "api_key": st.session_state.api_keys.get(st.session_state.llm_provider, ""),
    }

    # FIX 1: Move detect_button and run_button initialization here
    detect_button = None
    run_button = None
    
    # Geometric Spider: Detect Patterns button
    if spider_key == "geometric":
        detect_button = st.button("🔍 Detect Patterns", key="detect_patterns_btn")
        run_button = st.button("🚀 Run Spider", key="run_spider_btn")
        
        # FIX 2: Fix geometric spider pattern detection logic
        if detect_button:
            has_url = False
            url_value = ""
            for key, config in spider_config["params"].items():
                if config.get("required") and not params.get(key):
                    st.error(f"Required parameter: {config['label']}")
                    st.stop()
                if key in ['urls', 'start_url'] and params.get(key):
                    has_url = True
                    url_value = params.get(key, "").split(',')[0].strip()
            if not has_url:
                st.error("Please provide a URL to crawl")
                st.stop()
            st.markdown("---")
            st.subheader("🔍 Detecting Link Patterns")
            try:
                from selectolax.parser import HTMLParser
                import re
                def extract_link_patterns(page_url):
                    try:
                        resp = requests.get(page_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
                        html = resp.text
                        tree = HTMLParser(html)
                        links = [a.attributes.get("href", "") for a in tree.css("a") if a.attributes.get("href")]
                        pattern_examples = {}
                        for url in links:
                            pat = re.sub(r'(\d+|[a-zA-Z0-9]+)$', '', url.split('?')[0])
                            if pat not in pattern_examples:
                                pattern_examples[pat] = url
                        return sorted(pattern_examples.items())
                    except Exception as e:
                        st.error(f"Failed to fetch/analyze {page_url}: {e}")
                        return []
                detected_patterns = extract_link_patterns(url_value)
                st.session_state.detected_patterns = detected_patterns
                st.session_state.patterns_detected = True
                st.session_state.url_value = url_value
                st.rerun()
            except ImportError:
                st.error("selectolax not installed. Please install it with: pip install selectolax")
        
        # FIX 3: Show pattern selection UI for geometric spider
        if st.session_state.get("patterns_detected", False) and spider_key == "geometric":
            detected_patterns = st.session_state.get("detected_patterns", [])
            url_value = st.session_state.get("url_value", "")
            st.markdown("---")
            st.subheader("🔗 Select Patterns to Follow")
            follow_patterns = set()
            # Ensure detected_patterns is a list of tuples
            if isinstance(detected_patterns, dict):
                all_patterns = [(pat, example) for pat, example in detected_patterns.items()]
            else:
                all_patterns = detected_patterns
            seen = set()
            unique_patterns = []
            for pat, example in all_patterns:
                if pat not in seen:
                    unique_patterns.append((pat, example))
                    seen.add(pat)
            for idx, (pat, example) in enumerate(unique_patterns):
                safe_pat = pat.replace('/', '_').replace('\\', '_')
                key = f"geometric_follow_{idx}_{safe_pat}"
                checked = st.checkbox(f"Follow: {pat} (Example: {example})", value=True, key=key)
                if checked:
                    follow_patterns.add(pat)
            st.session_state.follow_patterns = follow_patterns
            
            # Suggest a follow pattern regex from selected patterns
            suggested_follow_pattern = "|".join([re.escape(pat) for pat in follow_patterns]) if follow_patterns else ""
            suggested_exclude_pattern = r"/category/|/tag/"
            
            st.markdown("---")
            st.subheader("✏️ Edit Patterns (Regex)")
            st.session_state.follow_pattern_regex = st.text_input(
                "Follow Pattern (Regex)",
                value=suggested_follow_pattern,
                help="Regex for links to follow"
            )
            st.session_state.exclude_pattern_regex = st.text_input(
                "Exclude Pattern (Regex)",
                value=suggested_exclude_pattern,
                help="Regex for links to exclude"
            )
        
        # FIX 4: Run geometric spider
        if run_button and spider_key == "geometric":
            # Get URL
            url_value = ""
            for key in ['urls', 'start_url']:
                if params.get(key):
                    url_value = params.get(key, "").split(',')[0].strip()
                    break
            
            if not url_value:
                st.error("Please provide a URL to crawl")
                st.stop()
            
            # Add follow patterns if detected
            if st.session_state.get("patterns_detected", False):
                follow_patterns = st.session_state.get("follow_patterns", set())
                follow_pattern_regex = st.session_state.get("follow_pattern_regex", "")
                exclude_pattern_regex = st.session_state.get("exclude_pattern_regex", "")
                
                if follow_pattern_regex:
                    params["follow_patterns"] = follow_pattern_regex
                elif follow_patterns:
                    params["follow_patterns"] = "|".join(follow_patterns)
                
                if exclude_pattern_regex:
                    params["exclude_patterns"] = exclude_pattern_regex

            # Apply benchmark/parallel profile from sidebar preset.
            profile = st.session_state.get("parallel_config")
            if profile:
                params["cores"] = int(profile.get("cores", params.get("cores", 4)))
                params["concurrent_requests"] = int(profile.get("concurrent_requests", params.get("concurrent_requests", 16)))
            if params.get("cores"):
                st.info(f"⚡ Parallel extraction enabled with {params['cores']} cores")
            if params.get("concurrent_requests"):
                st.info(f"🌐 {params['concurrent_requests']} concurrent requests configured")
        
            final_output_path = get_output_path(
                url=url_value,
                spider_key=spider_key,
                output_format=output_format,
                auto_name=auto_name_by_domain,
                organize=organize_by_domain,
                new_file=new_file_always
            )
            
            st.session_state.is_running = True
            st.session_state.output_log = []
            st.session_state.output_file = final_output_path
            st.info(f"📁 Output will be saved to: `{final_output_path}`")
            
            cmd = build_command(spider_key, params, final_output_path, output_format, llm_config if llm_config["enabled"] else None)
            effective = summarize_effective_scrapy_settings(cmd)
            st.info(
                "Effective parallel settings: "
                f"cores={st.session_state.get('parallel_config', {}).get('cores', params.get('cores', 4))}, "
                f"CONCURRENT_REQUESTS={effective.get('CONCURRENT_REQUESTS', 'n/a')}, "
                f"CONCURRENT_REQUESTS_PER_DOMAIN={effective.get('CONCURRENT_REQUESTS_PER_DOMAIN', 'n/a')}, "
                f"DOWNLOAD_DELAY={effective.get('DOWNLOAD_DELAY', 'n/a')}, "
                f"JOBDIR={effective.get('JOBDIR', 'n/a')}"
            )
            render_resume_status(spider_key, params, effective)

            mongo_enabled_for_run = st.session_state.mongo_enabled
            if mongo_enabled_for_run and not check_mongo_running(st.session_state.mongo_uri):
                st.warning("MongoDB is enabled in UI but not reachable. Continuing crawl with MongoDB disabled for this run.")
                mongo_enabled_for_run = False
            
            extra_env = {
                "OUTPUT_FORMAT": output_format,
                "MONGO_ENABLED": "true" if mongo_enabled_for_run else "false",
                "MONGO_URI": st.session_state.mongo_uri,
                "MONGO_DATABASE": st.session_state.mongo_database,
                "MONGO_COLLECTION": st.session_state.mongo_collection,
            }
            if output_format == "csv" and hasattr(st.session_state, "csv_output_columns"):
                csv_cols = st.session_state.csv_output_columns
                if csv_cols:
                    extra_env["CSV_OUTPUT_COLUMNS"] = ",".join(csv_cols)
            
            output_queue = queue.Queue()
            run_started_at = time.time()
            st.session_state.run_started_at = run_started_at
            st.session_state.run_ended_at = None
            thread = threading.Thread(target=run_spider, args=(cmd, output_queue, extra_env))
            thread.start()
            
            log_placeholder = st.empty()
            exit_code = None
            
            while thread.is_alive() or not output_queue.empty():
                try:
                    line = output_queue.get(timeout=0.1)
                    
                    if line.startswith("__EXIT_CODE_"):
                        exit_code = int(line.replace("__EXIT_CODE_", "").replace("__", ""))
                        if exit_code == 0:
                            st.session_state.output_log.append("✅ Spider completed successfully!")
                        else:
                            st.session_state.output_log.append(f"❌ Spider exited with code {exit_code}")
                    elif line.startswith("__ERROR__"):
                        error = line.replace("__ERROR__", "").replace("__", "")
                        st.session_state.output_log.append(f"❌ Error: {error}")
                    else:
                        st.session_state.output_log.append(line)
                    
                    log_placeholder.code("\n".join(st.session_state.output_log[-50:]), language="text")
                    
                except queue.Empty:
                    continue
            
            thread.join()
            st.session_state.is_running = False
            st.session_state.run_ended_at = time.time()
            run_metrics = extract_runtime_metrics(st.session_state.output_log, run_started_at, time.time())
            render_runtime_metrics(run_metrics)
            log_path = write_persistent_run_log(st.session_state.output_log, run_metrics, exit_code, cmd)
            st.caption(f"Run log saved: `{log_path}`")
            
            # Load results - try exact path first, then search for latest file
            result_loaded = False
            domain = extract_domain(url_value)
            if domain:
                date_str = datetime.now().strftime("%Y%m%d")
                json_dir = f"outputs/{domain}/json"
                csv_dir = f"outputs/{domain}/csv"
                latest_json = find_latest_output(json_dir, f"{domain}_{date_str}*.json*")
                latest_csv = find_latest_output(csv_dir, f"{domain}_{date_str}*.csv")
                potential_paths = [p for p in [latest_json, latest_csv, final_output_path] if p]
                
                for check_path in potential_paths:
                    if os.path.exists(check_path) and os.path.getsize(check_path) > 0:
                        st.session_state.results = load_results(check_path)
                        st.session_state.output_file = check_path
                        st.success(f"📁 Results loaded from: `{check_path}`")
                        result_loaded = True
                        break
            
            if not result_loaded and os.path.exists(final_output_path):
                st.session_state.results = load_results(final_output_path)
                st.session_state.output_file = final_output_path
                result_loaded = True
            
            if not result_loaded:
                st.warning("⚠️ No output file found. Check the Log tab for details.")

    # Drug Spider: Detect Patterns & Run
    if spider_key == "drug":
        detect_button = st.button("🔍 Detect Patterns", key="detect_patterns_btn")
        run_button = st.button("🚀 Run Spider", key="run_spider_btn")
        
        # FIX 5: Fix drug spider pattern detection
        if detect_button:
            has_url = False
            url_value = ""
            for key, config in spider_config["params"].items():
                if config.get("required") and not params.get(key):
                    st.error(f"Required parameter: {config['label']}")
                    st.stop()
                if key in ['urls', 'start_url'] and params.get(key):
                    has_url = True
                    url_value = params.get(key, "").split(',')[0].strip()
            if not has_url:
                st.error("Please provide a URL to crawl")
                st.stop()
            st.markdown("---")
            st.subheader("🔍 Detecting Link Patterns for Drug Spider")
            try:
                from selectolax.parser import HTMLParser
                import re
                def extract_link_patterns(page_url):
                    try:
                        resp = requests.get(page_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
                        html = resp.text
                        tree = HTMLParser(html)
                        links = [a.attributes.get("href", "") for a in tree.css("a") if a.attributes.get("href")]
                        pattern_examples = {}
                        for url in links:
                            pat = re.sub(r'(\d+|[a-zA-Z0-9]+)$', '', url.split('?')[0])
                            if pat not in pattern_examples:
                                pattern_examples[pat] = url
                        return sorted(pattern_examples.items())
                    except Exception as e:
                        st.error(f"Failed to fetch/analyze {page_url}: {e}")
                        return []
                detected_patterns = extract_link_patterns(url_value)
                st.session_state.detected_patterns = detected_patterns
                st.session_state.patterns_detected = True
                st.session_state.url_value = url_value
                st.rerun()
            except ImportError:
                st.error("selectolax not installed. Please install it with: pip install selectolax")
        
        # FIX 6: Show pattern selection UI for drug spider
        if st.session_state.get("patterns_detected", False) and spider_key == "drug":
            detected_patterns = st.session_state.get("detected_patterns", [])
            url_value = st.session_state.get("url_value", "")
            st.markdown("---")
            st.subheader("🔗 Select Patterns to Follow")
            follow_patterns = set()
            if isinstance(detected_patterns, dict):
                all_patterns = [(pat, example) for pat, example in detected_patterns.items()]
            else:
                all_patterns = detected_patterns
            seen = set()
            unique_patterns = []
            for pat, example in all_patterns:
                if pat not in seen:
                    unique_patterns.append((pat, example))
                    seen.add(pat)
            for idx, (pat, example) in enumerate(unique_patterns):
                safe_pat = pat.replace('/', '_').replace('\\', '_')
                key = f"popup_follow_{idx}_{safe_pat}"
                checked = st.checkbox(f"Follow: {pat} (Example: {example})", value=True, key=key)
                if checked:
                    follow_patterns.add(pat)
            st.session_state.follow_patterns = follow_patterns
            
            suggested_follow_pattern = "|".join([re.escape(pat) for pat in follow_patterns]) if follow_patterns else ""
            suggested_exclude_pattern = r"/pharmacy/|_pharmacies\\.htm$"
            
            st.markdown("---")
            st.subheader("✏️ Edit Patterns (Regex)")
            st.session_state.follow_pattern_regex = st.text_input(
                "Follow Pattern (Regex)",
                value=suggested_follow_pattern,
                help="Regex for links to follow"
            )
            st.session_state.exclude_pattern_regex = st.text_input(
                "Exclude Pattern (Regex)",
                value=suggested_exclude_pattern,
                help="Regex for links to exclude"
            )
        
        # FIX 7: Run drug spider
        if run_button and spider_key == "drug":
            url_value = params.get('start_url', '') or params.get('urls', '').split(',')[0].strip()
            
            if not url_value:
                st.error("Please provide a URL to crawl")
                st.stop()
            
            # Add patterns if detected
            if st.session_state.get("patterns_detected", False):
                follow_pattern_regex = st.session_state.get("follow_pattern_regex", "")
                exclude_pattern_regex = st.session_state.get("exclude_pattern_regex", "")
                follow_patterns = st.session_state.get("follow_patterns", set())
                
                if follow_pattern_regex:
                    params["link_pattern"] = follow_pattern_regex
                elif follow_patterns:
                    params["link_pattern"] = "|".join(follow_patterns)
                
                if exclude_pattern_regex:
                    params["exclude_pattern"] = exclude_pattern_regex

            # Apply benchmark/parallel profile from sidebar preset.
            profile = st.session_state.get("parallel_config")
            if profile:
                params["cores"] = int(profile.get("cores", params.get("cores", 4)))
                params["concurrent_requests"] = int(profile.get("concurrent_requests", params.get("concurrent_requests", 8)))
                    
            if params.get("concurrent_requests"):
                st.info(f" {params['concurrent_requests']} concurrent requests configured")
            if params.get("cores"):
                st.info(f" {params['cores']} parallel cores configured")
    
            final_output_path = get_output_path(
                url=url_value,
                spider_key=spider_key,
                output_format=output_format,
                auto_name=auto_name_by_domain,
                organize=organize_by_domain,
                new_file=new_file_always
            )
            
            st.session_state.is_running = True
            st.session_state.output_log = []
            st.session_state.output_file = final_output_path
            st.info(f"📁 Output will be saved to: `{final_output_path}`")
            
            cmd = build_command(spider_key, params, final_output_path, output_format, llm_config if llm_config["enabled"] else None)
            effective = summarize_effective_scrapy_settings(cmd)
            st.info(
                "Effective parallel settings: "
                f"cores={st.session_state.get('parallel_config', {}).get('cores', params.get('cores', 4))}, "
                f"CONCURRENT_REQUESTS={effective.get('CONCURRENT_REQUESTS', 'n/a')}, "
                f"CONCURRENT_REQUESTS_PER_DOMAIN={effective.get('CONCURRENT_REQUESTS_PER_DOMAIN', 'n/a')}, "
                f"DOWNLOAD_DELAY={effective.get('DOWNLOAD_DELAY', 'n/a')}, "
                f"JOBDIR={effective.get('JOBDIR', 'n/a')}"
            )
            render_resume_status(spider_key, params, effective)

            mongo_enabled_for_run = st.session_state.mongo_enabled
            if mongo_enabled_for_run and not check_mongo_running(st.session_state.mongo_uri):
                st.warning("MongoDB is enabled in UI but not reachable. Continuing crawl with MongoDB disabled for this run.")
                mongo_enabled_for_run = False
            
            extra_env = {
                "MONGO_ENABLED": "true" if mongo_enabled_for_run else "false",
                "MONGO_URI": st.session_state.mongo_uri,
                "MONGO_DATABASE": st.session_state.mongo_database,
                "MONGO_COLLECTION": st.session_state.mongo_collection,
            }
            if output_format == "csv" and hasattr(st.session_state, "csv_output_columns"):
                csv_cols = st.session_state.csv_output_columns
                if csv_cols:
                    extra_env["CSV_OUTPUT_COLUMNS"] = ",".join(csv_cols)
            
            output_queue = queue.Queue()
            run_started_at = time.time()
            st.session_state.run_started_at = run_started_at
            st.session_state.run_ended_at = None
            thread = threading.Thread(target=run_spider, args=(cmd, output_queue, extra_env))
            thread.start()
            
            log_placeholder = st.empty()
            exit_code = None
            
            while thread.is_alive() or not output_queue.empty():
                try:
                    line = output_queue.get(timeout=0.1)
                    
                    if line.startswith("__EXIT_CODE_"):
                        exit_code = int(line.replace("__EXIT_CODE_", "").replace("__", ""))
                        if exit_code == 0:
                            st.session_state.output_log.append("✅ Spider completed successfully!")
                        else:
                            st.session_state.output_log.append(f"❌ Spider exited with code {exit_code}")
                    elif line.startswith("__ERROR__"):
                        error = line.replace("__ERROR__", "").replace("__", "")
                        st.session_state.output_log.append(f"❌ Error: {error}")
                    else:
                        st.session_state.output_log.append(line)
                    
                    log_placeholder.code("\n".join(st.session_state.output_log[-50:]), language="text")
                    
                except queue.Empty:
                    continue
            
            thread.join()
            st.session_state.is_running = False
            st.session_state.run_ended_at = time.time()
            run_metrics = extract_runtime_metrics(st.session_state.output_log, run_started_at, time.time())
            render_runtime_metrics(run_metrics)
            log_path = write_persistent_run_log(st.session_state.output_log, run_metrics, exit_code, cmd)
            st.caption(f"Run log saved: `{log_path}`")
            
            # Load results
            result_loaded = False
            domain = extract_domain(url_value)
            if domain:
                date_str = datetime.now().strftime("%Y%m%d")
                json_dir = f"outputs/{domain}/json"
                csv_dir = f"outputs/{domain}/csv"
                latest_json = find_latest_output(json_dir, f"{domain}_{date_str}*.json*")
                latest_csv = find_latest_output(csv_dir, f"{domain}_{date_str}*.csv")
                spider_json_dir = f"outputs/{spider_key}/json"
                spider_csv_dir = f"outputs/{spider_key}/csv"
                latest_spider_json = find_latest_output(spider_json_dir, f"*_{date_str}*.json*")
                latest_spider_csv = find_latest_output(spider_csv_dir, f"*_{date_str}*.csv")
                potential_paths = [p for p in [latest_json, latest_spider_json, latest_csv, latest_spider_csv, final_output_path] if p]
                
                for check_path in potential_paths:
                    if os.path.exists(check_path) and os.path.getsize(check_path) > 0:
                        st.session_state.results = load_results(check_path)
                        st.session_state.output_file = check_path
                        st.success(f"📁 Results loaded from: `{check_path}`")
                        result_loaded = True
                        break
            
            if not result_loaded and os.path.exists(final_output_path):
                st.session_state.results = load_results(final_output_path)
                st.session_state.output_file = final_output_path
                result_loaded = True
            
            if not result_loaded:
                st.warning("⚠️ No output file found. Check the Log tab for details.")

    # MedlinePlus Spider: Run Spider
    if spider_key == "medlineplus":
        run_button = st.button("🚀 Run Spider", key="run_spider_btn")
        
        if run_button:
            url_value = params.get('urls', '').split(',')[0].strip() if params.get('urls') else ""
            
            if not url_value:
                st.warning("No URL provided. Using default MedlinePlus start URL.")
                url_value = "https://medlineplus.gov/druginfo/meds/a682878.html"
                params['urls'] = url_value

            # Apply benchmark/parallel profile from sidebar preset.
            profile = st.session_state.get("parallel_config")
            if profile:
                params["cores"] = int(profile.get("cores", params.get("cores", 4)))
                params["concurrent_requests"] = int(profile.get("concurrent_requests", params.get("concurrent_requests", 4)))
            
            final_output_path = get_output_path(
                url=url_value,
                spider_key=spider_key,
                output_format=output_format,
                auto_name=auto_name_by_domain,
                organize=organize_by_domain,
                new_file=new_file_always
            )
            
            st.session_state.is_running = True
            st.session_state.output_log = []
            st.session_state.output_file = final_output_path
            st.info(f"📁 Output will be saved to: `{final_output_path}`")
            
            cmd = build_command(spider_key, params, final_output_path, output_format, llm_config if llm_config["enabled"] else None)
            effective = summarize_effective_scrapy_settings(cmd)
            st.info(
                "Effective parallel settings: "
                f"cores={st.session_state.get('parallel_config', {}).get('cores', params.get('cores', 4))}, "
                f"CONCURRENT_REQUESTS={effective.get('CONCURRENT_REQUESTS', 'n/a')}, "
                f"CONCURRENT_REQUESTS_PER_DOMAIN={effective.get('CONCURRENT_REQUESTS_PER_DOMAIN', 'n/a')}, "
                f"DOWNLOAD_DELAY={effective.get('DOWNLOAD_DELAY', 'n/a')}, "
                f"JOBDIR={effective.get('JOBDIR', 'n/a')}"
            )
            render_resume_status(spider_key, params, effective)

            mongo_enabled_for_run = st.session_state.mongo_enabled
            if mongo_enabled_for_run and not check_mongo_running(st.session_state.mongo_uri):
                st.warning("MongoDB is enabled in UI but not reachable. Continuing crawl with MongoDB disabled for this run.")
                mongo_enabled_for_run = False
            
            extra_env = {
                "MONGO_ENABLED": "true" if mongo_enabled_for_run else "false",
                "MONGO_URI": st.session_state.mongo_uri,
                "MONGO_DATABASE": st.session_state.mongo_database,
                "MONGO_COLLECTION": st.session_state.mongo_collection,
            }
            if output_format == "csv" and hasattr(st.session_state, "csv_output_columns"):
                csv_cols = st.session_state.csv_output_columns
                if csv_cols:
                    extra_env["CSV_OUTPUT_COLUMNS"] = ",".join(csv_cols)
            
            output_queue = queue.Queue()
            run_started_at = time.time()
            st.session_state.run_started_at = run_started_at
            st.session_state.run_ended_at = None
            thread = threading.Thread(target=run_spider, args=(cmd, output_queue, extra_env))
            thread.start()
            
            log_placeholder = st.empty()
            exit_code = None
            
            while thread.is_alive() or not output_queue.empty():
                try:
                    line = output_queue.get(timeout=0.1)
                    
                    if line.startswith("__EXIT_CODE_"):
                        exit_code = int(line.replace("__EXIT_CODE_", "").replace("__", ""))
                        if exit_code == 0:
                            st.session_state.output_log.append("✅ Spider completed successfully!")
                        else:
                            st.session_state.output_log.append(f"❌ Spider exited with code {exit_code}")
                    elif line.startswith("__ERROR__"):
                        error = line.replace("__ERROR__", "").replace("__", "")
                        st.session_state.output_log.append(f"❌ Error: {error}")
                    else:
                        st.session_state.output_log.append(line)
                    
                    log_placeholder.code("\n".join(st.session_state.output_log[-50:]), language="text")
                    
                except queue.Empty:
                    continue
            
            thread.join()
            st.session_state.is_running = False
            st.session_state.run_ended_at = time.time()
            run_metrics = extract_runtime_metrics(st.session_state.output_log, run_started_at, time.time())
            render_runtime_metrics(run_metrics)
            log_path = write_persistent_run_log(st.session_state.output_log, run_metrics, exit_code, cmd)
            st.caption(f"Run log saved: `{log_path}`")
            
            # Load results - try exact path first, then search for latest file
            result_loaded = False
            domain = extract_domain(url_value)
            if domain:
                date_str = datetime.now().strftime("%Y%m%d")
                json_dir = f"outputs/{domain}/json"
                csv_dir = f"outputs/{domain}/csv"
                latest_json = find_latest_output(json_dir, f"{domain}_{date_str}*.json*")
                latest_csv = find_latest_output(csv_dir, f"{domain}_{date_str}*.csv")
                potential_paths = [p for p in [latest_json, latest_csv, final_output_path] if p]
                
                for check_path in potential_paths:
                    if os.path.exists(check_path) and os.path.getsize(check_path) > 0:
                        st.session_state.results = load_results(check_path)
                        st.session_state.output_file = check_path
                        st.success(f"📁 Results loaded from: `{check_path}`")
                        result_loaded = True
                        break
            
            if not result_loaded and os.path.exists(final_output_path):
                st.session_state.results = load_results(final_output_path)
                st.session_state.output_file = final_output_path
                result_loaded = True
            
            if not result_loaded:
                st.warning("⚠️ No output file found. Check the Log tab for details.")

with tab2:
    st.subheader("📊 Extraction Results")
    
    if st.session_state.results is not None and not st.session_state.results.empty:
        df = st.session_state.results
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Items", len(df))
        with col2:
            data_cols = [c for c in df.columns if c.startswith("data_")]
            st.metric("Data Fields", len(data_cols))
        with col3:
            if st.session_state.output_file:
                try:
                    file_size = os.path.getsize(st.session_state.output_file) / 1024
                    st.metric("File Size", f"{file_size:.1f} KB")
                except FileNotFoundError:
                    st.warning(f"Output file not found: {st.session_state.output_file}. A new file will be created on next run.")
        
        st.markdown("---")
        
        all_columns = list(df.columns)
        selected_columns = st.multiselect(
            "Show Columns",
            options=all_columns,
            default=[c for c in all_columns if not c.startswith("data_") or c in ["data_drug_name", "data_uses", "data_side_effects"]],
        )
        
        if selected_columns:
            st.dataframe(df[selected_columns], use_container_width=True)
        else:
            st.dataframe(df, use_container_width=True)
        
        col1, col2 = st.columns(2)
        with col1:
            csv_data = df.to_csv(index=False)
            st.download_button(
                "📥 Download CSV",
                data=csv_data,
                file_name=f"results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        with col2:
            json_data = df.to_json(orient="records", indent=2)
            st.download_button(
                "📥 Download JSON",
                data=json_data,
                file_name=f"results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )
        
        st.markdown("---")
        st.subheader("🔍 Item Detail View")
        
        if len(df) > 0:
            selected_idx = st.selectbox(
                "Select Item",
                options=range(len(df)),
                format_func=lambda x: f"Item {x+1}: {df.iloc[x].get('data_drug_name', df.iloc[x].get('url', 'Unknown'))[:50]}"
            )
            
            item = df.iloc[selected_idx]
            
            if spider_key == "drug":
                for category, fields in DRUG_CATEGORIES.items():
                    matching = [f"data_{f}" for f in fields if f"data_{f}" in item.index and pd.notna(item.get(f"data_{f}"))]
                    if matching:
                        with st.expander(f"📁 {category}", expanded=category == "Basic Info"):
                            for field in matching:
                                value = item.get(field)
                                if value and str(value).strip():
                                    st.markdown(f"**{field.replace('data_', '')}:**")
                                    st.text_area("", value=str(value)[:2000], height=100, key=f"detail_{selected_idx}_{field}", disabled=True)
            else:
                for col in item.index:
                    value = item.get(col)
                    try:
                        is_na = pd.isna(value) if not isinstance(value, (list, tuple, dict)) else False
                    except (ValueError, TypeError):
                        is_na = False
                    if value is not None and str(value).strip() and not is_na:
                        st.markdown(f"**{col}:**")
                        if len(str(value)) > 200:
                            st.text_area("", value=str(value)[:2000], height=100, key=f"detail_{selected_idx}_{col}", disabled=True)
                        else:
                            st.write(value)
    else:
        st.info("No results yet. Run a spider to see results here.")
        
        with st.expander("📋 Expected Output Structure"):
            st.json({
                "url": "https://example.com/drug",
                "domain": "example.com",
                "container_type": "drug_complete",
                "data": {
                    "drug_name": "Example Drug",
                    "uses": "Treatment of...",
                    "dosage": "500mg twice daily",
                    "side_effects": "Nausea, headache...",
                    "mechanism_of_action": "Works by...",
                    "pharmacokinetics": "Bioavailability...",
                }
            })

with tab3:
    st.subheader("📋 Execution Log")
    
    if st.session_state.output_log:
        started_at = st.session_state.run_started_at or time.time()
        ended_at = st.session_state.run_ended_at or time.time()
        tab_metrics = extract_runtime_metrics(st.session_state.output_log, started_at, ended_at)
        render_runtime_metrics(tab_metrics)

        col1, col2 = st.columns([3, 1])
        with col1:
            filter_text = st.text_input("Filter log", placeholder="Search in log...")
        with col2:
            show_all = st.checkbox("Show all lines", value=False)
        
        filtered_log = st.session_state.output_log
        if filter_text:
            filtered_log = [line for line in filtered_log if filter_text.lower() in line.lower()]
        
        if not show_all and len(filtered_log) > 100:
            filtered_log = filtered_log[-100:]
            st.info("Showing last 100 lines. Check 'Show all lines' to see full log.")
        
        log_html = []
        for line in filtered_log:
            if "ERROR" in line or "❌" in line:
                log_html.append(f'<span style="color: #ff6b6b;">{line}</span>')
            elif "WARNING" in line or "⚠️" in line:
                log_html.append(f'<span style="color: #feca57;">{line}</span>')
            elif "INFO" in line or "✅" in line or "💊" in line:
                log_html.append(f'<span style="color: #1dd1a1;">{line}</span>')
            else:
                log_html.append(f'<span>{line}</span>')
        
        st.markdown(
            f'<div class="output-box">{"<br>".join(log_html)}</div>',
            unsafe_allow_html=True
        )
        
        if st.button("📥 Export Log"):
            log_text = "\n".join(st.session_state.output_log)
            st.download_button(
                "Download Log File",
                data=log_text,
                file_name=f"crawler_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                mime="text/plain"
            )
    else:
        st.info("No log entries yet. Run a spider to see execution log here.")

with tab4:
    st.subheader("🧬 Spider Generator - Single URL Input")
    st.markdown("""
    **Enter URLs for each page type to generate a custom spider:**
    """)

    try:
        import requests
        from selectolax.parser import HTMLParser
        import re
    except ImportError:
        st.error("Required modules not installed. Please install: pip install requests selectolax")

    def extract_link_patterns(page_url):
        try:
            resp = requests.get(page_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            html = resp.text
            tree = HTMLParser(html)
            links = [a.attributes.get("href", "") for a in tree.css("a") if a.attributes.get("href")]
            pattern_examples = {}
            for url in links:
                pat = re.sub(r'(\d+|[a-zA-Z0-9]+)$', '', url.split('?')[0])
                if pat not in pattern_examples:
                    pattern_examples[pat] = url
            return sorted(pattern_examples.items())
        except Exception as e:
            st.error(f"Failed to fetch/analyze {page_url}: {e}")
            return []

    def extract_sections(page_url):
        try:
            resp = requests.get(page_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            html = resp.text
            tree = HTMLParser(html)
            headings = []
            for tag in ["h1", "h2", "h3", "h4"]:
                for node in tree.css(tag):
                    txt = node.text(strip=True)
                    if txt and len(txt) > 2:
                        headings.append(txt)
            for node in tree.css(".DrugOverview__content___22ZBX h2, .DrugOverview__content___22ZBX h3"):
                txt = node.text(strip=True)
                if txt and txt not in headings:
                    headings.append(txt)
            norm_sections = set()
            for s in headings:
                m = re.match(r"([A-Za-z ]+)(?: of .+)?", s)
                if m:
                    norm_sections.add(m.group(1).strip())
                else:
                    norm_sections.add(s.strip())
            return sorted(norm_sections)
        except Exception as e:
            st.error(f"Failed to fetch/analyze sections from {page_url}: {e}")
            return []

    # Single set of URL inputs
    col1, col2, col3 = st.columns(3)
    with col1:
        master_url = st.text_input("📋 Master/List Page URL", key="gen_master_url", help="URL of category/index page with links to items")
    with col2:
        list_url = st.text_input("📄 Product/Card Page URL", key="gen_list_url", help="URL of a product listing/card page")
    with col3:
        detail_url = st.text_input("🔍 Detail Page URL", key="gen_detail_url", help="URL of a detailed product/drug page")

    # Pagination detection
    pagination_detected = False
    pagination_pattern = None
    
    for url in [master_url, list_url, detail_url]:
        if url and re.search(r'(page=|page/|p=|start=|offset=)\d+', url, re.IGNORECASE):
            pagination_detected = True
            pagination_match = re.search(r'(page=|page/|p=|start=|offset=)', url, re.IGNORECASE)
            if pagination_match:
                pagination_pattern = pagination_match.group(1)
            break

    if pagination_detected:
        st.info(f"📄 Pagination detected in URL(s) (pattern: '{pagination_pattern}'). Pagination will be followed.")
        follow_pagination = True
    else:
        follow_pagination = st.checkbox("Follow pagination?", value=False, key="gen_follow_pagination", 
                                        help="Check if you want the spider to follow paginated links.")

    # Analyze button
    if st.button("🔍 Analyze URLs", key="analyze_urls_btn", type="primary"):
        detected_patterns = {}
        if master_url:
            detected_patterns['master'] = extract_link_patterns(master_url)
        if list_url:
            detected_patterns['list'] = extract_link_patterns(list_url)
        if detail_url:
            detected_patterns['detail'] = extract_link_patterns(detail_url)
            detected_sections = extract_sections(detail_url)
            st.session_state.detected_sections = detected_sections
        st.session_state.detected_patterns = detected_patterns

    # Display detected patterns
    st.markdown("**LLM-based Spider Generation Only**")
    if not st.session_state.get('llm_enabled', False):
        st.warning("LLM must be enabled to use spider generation in this section. Please enable LLM Repair in the sidebar.")
    else:
        if 'detected_patterns' in st.session_state:
            detected_patterns = st.session_state.detected_patterns
            st.markdown("### 🔗 Detected Link Patterns (LLM-based)")
            follow_patterns = []
            all_patterns = []
            if isinstance(detected_patterns, dict):
                for pattern_list in detected_patterns.values():
                    all_patterns.extend(pattern_list)
            else:
                all_patterns = detected_patterns
            for idx, (pat, example) in enumerate(all_patterns):
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.code(f"Pattern: {pat}\nExample: {example}")
                with col2:
                    unique_key = f"follow_{idx}_{pat}_{example}"
                    if st.checkbox("Follow", key=unique_key, value=True):
                        follow_patterns.append(pat)
            st.session_state.follow_patterns = follow_patterns

    # Display detected sections
    if 'detected_sections' in st.session_state and st.session_state.detected_sections:
        st.markdown("### 📑 Detected Sections to Extract")
        selected_sections = st.multiselect(
            "Select sections to extract:",
            st.session_state.detected_sections,
            default=st.session_state.detected_sections,
            key="gen_selected_sections"
        )
        st.session_state.selected_sections = selected_sections

    # Generate spider button
    llm_enabled = st.session_state.get('llm_enabled', False)
    llm_provider = st.session_state.get('llm_provider', '')
    ollama_running = st.session_state.get('ollama_running', True) if llm_provider == 'ollama' else True
    generate_disabled = not llm_enabled or not ollama_running
    if generate_disabled:
        st.warning("LLM must be enabled and running to generate a spider. Please enable LLM Repair and ensure the LLM server is running.")
    generate_btn = st.button("🕷️ Generate Spider", key="generate_spider_btn", type="primary", disabled=generate_disabled)
    parallel_config = st.session_state.get("parallel_config", {
        "concurrent_requests": 16,
        "download_delay": 0.25,
        "concurrent_requests_per_domain": 8,
        "cores": 4,
    })
    if generate_btn:
        if not master_url or not list_url or not detail_url:
            st.error("Please provide all three URLs (Master, List, and Detail pages)")
        else:
            st.info("⏳ Sending site URLs and unique detected patterns to LLM for spider generation...")
            try:
                from site_extractor import generate_spider_config_llm
                domain = extract_domain(master_url)
                # Deduplicate follow_patterns by pattern prefix
                follow_patterns = st.session_state.get('follow_patterns', [])
                unique_patterns = {}
                for pat in follow_patterns:
                    key = pat.split('-', 2)[0] if '-' in pat else pat
                    if key not in unique_patterns:
                        unique_patterns[key] = pat
                deduped_patterns = list(unique_patterns.values())
                selected_sections = st.session_state.get('selected_sections', [])
                max_retries = 3
                config_json, llm_output = None, None
                for attempt in range(1, max_retries + 1):
                    st.info(f"LLM attempt {attempt}...")
                    try:
                        _, llm_output = generate_spider_config_llm(
                            master_url, list_url, detail_url,
                            deduped_patterns, selected_sections,
                            pagination_pattern, follow_pagination,
                            llm_model=st.session_state.llm_model or "llama2",
                            timeout=180
                        )
                        if llm_output:
                            st.session_state.llm_output = llm_output
                            st.success("✅ LLM spider code generated! Review and save as a spider file below.")
                            break
                    except Exception as e:
                        st.warning(f"LLM call failed (attempt {attempt}): {e}")
                        time.sleep(2)
                if 'llm_output' in st.session_state and st.session_state.llm_output:
                    st.info("LLM output is available as code. Review and save as a spider file if correct.")
                else:
                    st.error("LLM did not return a valid spider after multiple attempts. Please check the LLM output and server logs.")
                    if llm_output:
                        st.text_area("LLM Output", value=str(llm_output), height=200)
            except ImportError:
                st.error("site_extractor module not found. Please ensure it exists.")

    # Display generated config
    if 'generated_config' in st.session_state and st.session_state.generated_config:
        # Show Python code preview and save button
        if 'llm_output' in st.session_state and st.session_state.llm_output:
            st.markdown("---")
            st.markdown("### 🐍 Generated Spider Code Preview")
            st.code(st.session_state.llm_output, language="python")
            domain = extract_domain(master_url) if master_url else "spider"
            if st.button("💾 Save Spider File", key="save_llm_spider_btn"):
                spider_code = st.session_state.llm_output
                spider_name = domain.replace('.', '_') + "_llm_spider.py"
                spider_path = Path("geometric_crawler/spiders") / spider_name
                Path("geometric_crawler/spiders").mkdir(parents=True, exist_ok=True)
                with open(spider_path, "w", encoding="utf-8") as f:
                    f.write(spider_code)
                st.session_state.saved_spider_path = str(spider_path)
                st.success(f"✅ Spider file saved to {spider_path}")
                
                # Run Spider button
                st.markdown("---")
                run_spider_btn = st.button("🚀 Run Generated Spider", key="run_generated_spider_btn")
                if run_spider_btn:
                    if 'saved_spider_path' in st.session_state:
                        spider_path = st.session_state.saved_spider_path
                        spider_file = Path(spider_path).name
                        spider_name = spider_file.replace('.py', '')
                        st.info(f"⏳ Running generated spider: {spider_name} ...")
                        output_filename = f"output_{spider_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                        cmd = [
                            sys.executable, "-m", "scrapy", "crawl", spider_name,
                            "-o", output_filename,
                            "-s", "LOG_LEVEL=INFO",
                            "-s", f"CONCURRENT_REQUESTS={int(parallel_config.get('concurrent_requests', 16))}",
                            "-s", f"CONCURRENT_REQUESTS_PER_DOMAIN={int(parallel_config.get('concurrent_requests_per_domain', max(1, int(parallel_config.get('concurrent_requests', 16)) // 2)))}",
                            "-s", f"DOWNLOAD_DELAY={float(parallel_config.get('download_delay', 0.25))}",
                            "-s", "AUTOTHROTTLE_ENABLED=True",
                            "-s", "AUTOTHROTTLE_START_DELAY=0.25",
                            "-s", "AUTOTHROTTLE_MAX_DELAY=3.0",
                            "-s", "AUTOTHROTTLE_TARGET_CONCURRENCY=8.0",
                        ]
                        st.info(
                            "Effective parallel settings (generated spider): "
                            f"cores={int(parallel_config.get('cores', 4))}, "
                            f"CONCURRENT_REQUESTS={int(parallel_config.get('concurrent_requests', 16))}, "
                            f"CONCURRENT_REQUESTS_PER_DOMAIN={int(parallel_config.get('concurrent_requests_per_domain', max(1, int(parallel_config.get('concurrent_requests', 16)) // 2)))}, "
                            f"DOWNLOAD_DELAY={float(parallel_config.get('download_delay', 0.25))}"
                        )
                        output_queue = queue.Queue()
                        
                        def run_scrapy():
                            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=str(Path(__file__).parent))
                            for line in process.stdout:
                                output_queue.put(line)
                            process.wait()
                            output_queue.put(f"__EXIT_CODE_{process.returncode}__")
                        
                        thread = threading.Thread(target=run_scrapy)
                        thread.start()
                        log_placeholder = st.empty()
                        while thread.is_alive() or not output_queue.empty():
                            try:
                                line = output_queue.get(timeout=0.1)
                                if line.startswith("__EXIT_CODE_"):
                                    exit_code = int(line.replace("__EXIT_CODE_", "").replace("__", ""))
                                    if exit_code == 0:
                                        st.session_state.output_log.append("✅ Spider completed successfully!")
                                    else:
                                        st.session_state.output_log.append(f"❌ Spider exited with code {exit_code}")
                                elif line.startswith("__ERROR__"):
                                    error = line.replace("__ERROR__", "").replace("__", "")
                                    st.session_state.output_log.append(f"❌ Error: {error}")
                                else:
                                    st.session_state.output_log.append(line)
                                log_placeholder.code("\n".join(st.session_state.output_log[-50:]), language="text")
                            except queue.Empty:
                                continue
                        thread.join()
                        st.session_state.is_running = False
                        st.success(f"✅ Run complete. Output saved to: {output_filename}")
                    else:
                        st.error("Please save the spider file before running.")
    elif 'llm_output' in st.session_state and st.session_state.llm_output:
        # Show LLM output even if generated_config doesn't exist
        st.markdown("---")
        st.markdown("### 🐍 Generated Spider Code Preview")
        st.code(st.session_state.llm_output, language="python")
        domain = extract_domain(master_url) if master_url else "spider"
        if st.button("💾 Save Spider File", key="save_llm_spider_btn"):
            spider_code = st.session_state.llm_output
            spider_name = domain.replace('.', '_') + "_llm_spider.py"
            spider_path = Path("geometric_crawler/spiders") / spider_name
            Path("geometric_crawler/spiders").mkdir(parents=True, exist_ok=True)
            with open(spider_path, "w", encoding="utf-8") as f:
                f.write(spider_code)
            st.session_state.saved_spider_path = str(spider_path)
            st.success(f"✅ Spider file saved to {spider_path}")
    
    # ===== Quick Site Extractor Panel =====
    if st.session_state.get("show_extractor", False):
        try:
            from site_extractor import render_extractor_panel
            render_extractor_panel()
        except ImportError:
            st.error("site_extractor module not found.")