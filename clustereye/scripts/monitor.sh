#!/usr/bin/env bash
# ClusterEye Resource Monitor - real-time system monitoring
# Usage: bash scripts/monitor.sh

BLUE='\033[1;34m'
GREEN='\033[1;32m'
YELLOW='\033[1;33m'
RED='\033[1;31m'
CYAN='\033[1;36m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

# Hide cursor, restore on exit
tput civis 2>/dev/null
trap 'tput cnorm 2>/dev/null; exit' INT TERM

LINES_PRINTED=0

bar() {
    local pct=$1 width=30 filled
    [ -z "$pct" ] && pct=0
    pct=$((pct > 100 ? 100 : pct))
    pct=$((pct < 0 ? 0 : pct))
    filled=$((pct * width / 100))
    printf "["
    for ((i=0; i<filled; i++)); do
        if [ "$pct" -gt 80 ]; then printf "${RED}#${NC}";
        elif [ "$pct" -gt 50 ]; then printf "${YELLOW}#${NC}";
        else printf "${GREEN}#${NC}"; fi
    done
    for ((i=filled; i<width; i++)); do printf " "; done
    printf "] %3d%%" "$pct"
}

# First run: print everything. After that: move cursor up and overwrite.
FIRST=1

while true; do
    output=""
    nl=$'\n'

    # Header
    output+="${BOLD}${BLUE}═══════════════════════════════════════════════════════════════${NC}${nl}"
    output+="${BOLD}${BLUE}  ClusterEye Resource Monitor   $(date '+%H:%M:%S')                        ${NC}${nl}"
    output+="${BOLD}${BLUE}═══════════════════════════════════════════════════════════════${NC}${nl}"
    output+="${nl}"

    # --- GPU ---
    if command -v nvidia-smi &>/dev/null; then
        gpu_info=$(nvidia-smi --query-gpu=name,utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw,power.limit --format=csv,noheader,nounits 2>/dev/null)
        if [ -n "$gpu_info" ]; then
            IFS=',' read -r gpu_name gpu_util mem_used mem_total gpu_temp power_draw power_limit <<< "$gpu_info"
            gpu_name=$(echo "$gpu_name" | xargs)
            gpu_util=$(echo "$gpu_util" | xargs | cut -d. -f1)
            mem_used=$(echo "$mem_used" | xargs | cut -d. -f1)
            mem_total=$(echo "$mem_total" | xargs | cut -d. -f1)
            gpu_temp=$(echo "$gpu_temp" | xargs | cut -d. -f1)
            power_draw=$(echo "$power_draw" | xargs | cut -d. -f1)
            power_limit=$(echo "$power_limit" | xargs | cut -d. -f1)
            mem_pct=$((mem_used * 100 / mem_total))

            output+="${BOLD}${CYAN}  GPU: ${gpu_name}${NC}${nl}"
            output+="  ├─ Compute : $(bar "$gpu_util")                ${nl}"
            output+="  ├─ VRAM    : $(bar "$mem_pct")  ${mem_used}/${mem_total} MiB    ${nl}"
            output+="  ├─ Temp    : ${gpu_temp}°C                              ${nl}"
            output+="  └─ Power   : ${power_draw}W / ${power_limit}W                  ${nl}"
            output+="${nl}"
        fi
    fi

    # --- CPU ---
    cpu_idle=$(top -bn1 2>/dev/null | grep '%Cpu' | awk '{print $8}' | cut -d. -f1)
    [ -z "$cpu_idle" ] && cpu_idle=100
    cpu_used=$((100 - cpu_idle))
    load=$(uptime | awk -F'load average:' '{print $2}' | xargs)
    cores=$(nproc)
    cpu_model=$(lscpu 2>/dev/null | grep 'Model name' | sed 's/Model name:\s*//')

    output+="${BOLD}${CYAN}  CPU: ${cpu_model}${NC}${nl}"
    output+="  ├─ Usage   : $(bar "$cpu_used")                ${nl}"
    output+="  ├─ Load    : ${load}  (${cores} threads)       ${nl}"
    output+="  └─ Cores   : ${cores}                          ${nl}"
    output+="${nl}"

    # --- RAM ---
    read -r ram_total ram_used ram_free ram_available <<< $(free -m | awk '/Mem:/{print $2, $3, $4, $7}')
    ram_pct=$((ram_used * 100 / ram_total))
    ram_total_gb=$((ram_total / 1024))
    ram_used_gb=$((ram_used / 1024))

    output+="${BOLD}${CYAN}  RAM${NC}${nl}"
    output+="  ├─ Usage   : $(bar "$ram_pct")  ${ram_used_gb}G / ${ram_total_gb}G     ${nl}"
    output+="  └─ Free    : $((ram_free / 1024))G available                ${nl}"
    output+="${nl}"

    # --- Processes ---
    output+="${BOLD}${CYAN}  Processes${NC}${nl}"

    # Ollama - use ps with specific format to avoid field splitting issues
    oll_pid=$(pgrep -f 'ollama runner' | head -1)
    if [ -n "$oll_pid" ]; then
        oll_cpu=$(ps -p "$oll_pid" -o %cpu= 2>/dev/null | xargs)
        oll_rss=$(ps -p "$oll_pid" -o rss= 2>/dev/null | xargs)
        oll_cpu_int=${oll_cpu%%.*}
        [ -z "$oll_cpu_int" ] && oll_cpu_int=0
        oll_mem_gb=$(awk "BEGIN{printf \"%.1f\", ${oll_rss:-0}/1048576}")
        output+="  ├─ ${GREEN}Ollama LLM${NC}    : CPU ${oll_cpu_int}%%  RAM ${oll_mem_gb}G  ${DIM}(model inference)${NC}     ${nl}"
    else
        output+="  ├─ ${DIM}Ollama LLM    : not running${NC}                       ${nl}"
    fi

    # FastAPI
    api_pid=$(pgrep -f 'uvicorn.*clustereye' | head -1)
    if [ -n "$api_pid" ]; then
        api_cpu=$(ps -p "$api_pid" -o %cpu= 2>/dev/null | xargs)
        output+="  ├─ ${GREEN}FastAPI${NC}       : CPU ${api_cpu}%%  ${DIM}(API server :8000)${NC}        ${nl}"
    else
        output+="  ├─ ${DIM}FastAPI       : not running${NC}                       ${nl}"
    fi

    # Streamlit
    st_pid=$(pgrep -f 'streamlit run' | head -1)
    if [ -n "$st_pid" ]; then
        st_cpu=$(ps -p "$st_pid" -o %cpu= 2>/dev/null | xargs)
        output+="  ├─ ${GREEN}Streamlit${NC}     : CPU ${st_cpu}%%  ${DIM}(Chat UI :8501)${NC}           ${nl}"
    else
        output+="  ├─ ${DIM}Streamlit     : not running${NC}                       ${nl}"
    fi

    # Qdrant
    docker_qdrant=$(docker ps --filter "ancestor=qdrant/qdrant" --format "{{.Status}}" 2>/dev/null | head -1)
    if [ -n "$docker_qdrant" ]; then
        output+="  └─ ${GREEN}Qdrant${NC}        : Docker (${docker_qdrant})  ${DIM}(:6333)${NC}  ${nl}"
    elif pgrep -f qdrant &>/dev/null; then
        output+="  └─ ${GREEN}Qdrant${NC}        : running  ${DIM}(vector DB :6333)${NC}       ${nl}"
    else
        output+="  └─ ${RED}Qdrant${NC}        : not running                       ${nl}"
    fi
    output+="${nl}"

    # --- Status ---
    output+="${BOLD}${BLUE}───────────────────────────────────────────────────────────────${NC}${nl}"
    if [ -n "$oll_pid" ]; then
        if [ "${oll_cpu_int:-0}" -gt 100 ]; then
            output+="  ${YELLOW}⟳ LLM generating response${NC}                                ${nl}"
            output+="  ${DIM}  32B model: GPU handles partial layers, CPU handles rest${NC}  ${nl}"
            output+="  ${DIM}  VRAM full → ${oll_mem_gb}G offloaded to RAM${NC}              ${nl}"
        elif [ "${gpu_util:-0}" -gt 50 ]; then
            output+="  ${GREEN}⟳ GPU inference active${NC}                                   ${nl}"
            output+="  ${DIM}  Model running primarily on GPU${NC}                        ${nl}"
            output+="                                                              ${nl}"
        else
            output+="  ${GREEN}✓ Ready — waiting for queries${NC}                             ${nl}"
            output+="  ${DIM}  Model loaded, no active inference${NC}                     ${nl}"
            output+="                                                              ${nl}"
        fi
    else
        output+="  ${DIM}  No model loaded${NC}                                       ${nl}"
        output+="                                                              ${nl}"
        output+="                                                              ${nl}"
    fi
    output+="${BOLD}${BLUE}═══════════════════════════════════════════════════════════════${NC}${nl}"
    output+="  ${DIM}Refresh: 2s | Ctrl+C to exit${NC}                              ${nl}"

    # Count lines
    line_count=$(echo "$output" | grep -c '')

    # Move cursor up to overwrite (skip on first run)
    if [ "$FIRST" -eq 1 ]; then
        FIRST=0
    else
        printf "\033[${LINES_PRINTED}A"
    fi

    LINES_PRINTED=$line_count
    echo -e "$output"

    sleep 2
done
