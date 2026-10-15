#!/usr/bin/env python3
"""Generate batch 3 entries (010+): TT&C / onboard computer / more real cases."""
import json, os
OUT = "docs/knowledge"

def write(name, entry):
    json.dump(entry, open(os.path.join(OUT, name), "w"), ensure_ascii=False, indent=2)
    print("written", name)

def base_ontology():
    return {
        "namespace": "tianta:aero",
        "root_class": "SpacecraftComponent",
        "classes": [
            {"name": "AttitudeControlSystem", "parent": "SpacecraftComponent", "components": ["ReactionWheel","MomentumWheel","StarSensor","Gyroscope","Magnetorquer"]},
            {"name": "ElectricalPowerSystem", "parent": "SpacecraftComponent", "components": ["SolarArray","Battery","PowerController"]},
            {"name": "ThermalControlSystem", "parent": "SpacecraftComponent", "components": ["Thermistor","Heater"]},
            {"name": "TelemetryTrackingCommand", "parent": "SpacecraftComponent", "components": ["Transponder","Transmitter","Antenna","Link"]},
            {"name": "Payload", "parent": "SpacecraftComponent", "components": ["Instrument"]},
            {"name": "CommandAndDataHandling", "parent": "SpacecraftComponent", "components": ["OnboardComputer","CommandProcessor","FlightSoftware","Clock"]},
            {"name": "GuidanceNavigationControl", "parent": "SpacecraftComponent", "components": ["InertialReferenceSystem"]},
        ],
        "instances": [], "properties": [], "relations": [],
    }

# =====================================================================
# 010 — TT&C 测控/通信链路故障 (generic)
# =====================================================================
ttc = {
  "schema_version": "1.0","knowledge_type":"ontology+simulation",
  "id":"SPACECRAFT-FAULT-010",
  "title":"测控/通信链路故障(通用)",
  "domain":["aerospace","ttc","communication"],
  "created":"2026-10-14",
  "provenance":{"source":["AnySearch retrieval"],
    "references":["TT&C tutorial (IEEE COMST 2023)","SATCOM recovery strategies (Waldon)","Satellite communication review"],
    "confidence":"high"},
  "ontology":{**base_ontology(),
    "relations":[{"type":"causes","from":"transponder_failure","to":"link_loss"},{"type":"causes","from":"antenna_mispoint","to":"signal_loss"}]},
  "fault_models":[
    {"fault_id":"F-TTC-TRANSPONDER","type":"CommunicationFailure","component":"Transponder","severity":"high",
      "symptom":{"telemetry":["link_signal","bit_error"],"description":"转发器失效/降级,链路中断,通信品质下降"},
      "cause":{"internal":["power_amplifier_failure","upconverter_drift","aging"],"environmental":["radiation"]},
      "recovery":["redundant_transponder_switch","power_managed_operation","spare_transponder_use"]},
    {"fault_id":"F-TTC-ANTENNA","type":"CommunicationFailure","component":"Antenna","severity":"high",
      "symptom":{"telemetry":["link_signal","pointing"],"description":"天线对地指向偏差/指向机构故障,信号衰减"},
      "cause":{"internal":["gimbal_failure","deployment_issue"],"environmental":[]},
      "recovery":["repoint_command","fallback_low_gain"]}],
  "simulation":{
    "state_space":{"states":["nominal","degraded","lost"],"transitions":{"nominal":["degraded"],"degraded":["lost"]}},
    "models":[{"model_id":"MOD-TTC","target":"Transponder","kind":"link_budget","state":["link_signal"],"input":["power"],
               "parameters":{"tx_power_W":50.0,"antenna_gain_dbi":30},"fault_injection":{"power_factor":0.1,"noise_temp_K":1000},"output":["link_signal"]}],
    "sensors":["link_signal","bit_error"],"time_resolution_ms":100.0,"duration_s":3600.0,
    "scenario_templates":[{"id":"SCN-TTC","fault":"F-TTC-TRANSPONDER","inject_at_s":600,"observables":["link_signal"]}]},
  "rules":[{"rule_id":"R1","if":"link_signal < -160dBm","then":"switch redundant transponder"}],
  "tags":["测控","通信","转发器","天线","链路","TT&C"]
}
write("spacecraft-fault-010-ttc.json", ttc)

# =====================================================================
# 011 — Onboard Computer 星载计算机故障
# =====================================================================
obc = {
  "schema_version": "1.0","knowledge_type":"ontology+simulation",
  "id":"SPACECRAFT-FAULT-011",
  "title":"星载计算机/数据处理故障",
  "domain":["aerospace","obc","software","computer"],
  "created":"2026-10-14",
  "provenance":{"source":["AnySearch retrieval"],
    "references":["Requirements-related software failures (including Therac-25/Ariane leaks)","Fault-tolerant real-time computer system (NASA Tech Briefs)","MSC robustness recovery"],
    "confidence":"high"},
  "ontology":{**base_ontology(),
    "relations":[{"type":"causes","from":"software_execption","to":"obc_reboot"}]},
  "fault_models":[
    {"fault_id":"F-OBC-LOCKUP","type":"OnboardComputerFailure","component":"OnboardComputer","severity":"high",
      "symptom":{"telemetry":["reboot_count","cpu_health"],"description":"星载计算机死锁/复位,指令处理中断,需看门狗重启"},
      "cause":{"internal":["software_exception","memory_corruption","unexpected_input"],"environmental":["SEU","single_event_latchup"]},
      "recovery":["watchdog_reboot","redundant_computer_switch","software_patch_reload","memory_scrub"]},
    {"fault_id":"F-OBC-CLOCK","type":"TimingFault","component":"CommandAndDataHandling","severity":"medium",
      "symptom":{"telemetry":["time_sync"],"description":"时钟漂移/失步,任务调度错乱"},
      "cause":{"internal":["oscillator_drift","clock_fault"],"environmental":[]},
      "recovery":["resync","redundant_oscillator"]}],
  "simulation":{
    "state_space":{"states":["running","suspended","reboot"],"transitions":{"running":["suspended"],"suspended":["reboot"]}},
    "models":[{"model_id":"MOD-OBC","target":"OnboardComputer","kind":"stm","state":["cpu_state"],"input":["commands"],
               "parameters":{"reboot_threshold":3,"uptime_s":0},"fault_injection":{"exception_rate":0.01},"output":["reboot_count"]}],
    "sensors":["reboot_count","cpu_health"],"time_resolution_ms":100.0,"duration_s":86400.0,
    "scenario_templates":[{"id":"SCN-OBC","fault":"F-OBC-LOCKUP","inject_at_s":1000,"observables":["reboot_count"]}]},
  "rules":[{"rule_id":"R1","if":"reboot_count > 3/min","then":"switch redundant computer"}],
  "tags":["星载计算机","OBC","软件","死锁","看门狗","复位"]
}
write("spacecraft-fault-011-obc.json", obc)

# =====================================================================
# 012 — Real case: synchronous communication satellite failure (generic-ish real)
# =====================================================================
comm_sat = {
  "schema_version": "1.0","knowledge_type":"ontology+simulation",
  "id":"SPACECRAFT-FAULT-012",
  "title":"真实案例:GEO通信卫星业务故障(转发器/轨道保持)",
  "domain":["aerospace","geostationary","communication-satellite","business-continuity"],
  "created":"2026-10-14",
  "provenance":{"source":["AnySearch retrieval"],
    "references":["Impact 1859 superstorm on GEO transponders","Strategies to recover from SATCOM failures","Express satellites station-keeping"],
    "confidence":"medium"},
  "ontology":{**base_ontology(),
    "relations":[{"type":"causes","from":"transponder_damage","to":"revenue_loss_broadcast_outage"}]},
  "fault_models":[
    {"fault_id":"F-GEO-TRANSP-NET","type":["CommunicationFailure","BusinessOutage"],"component":"Transponder","severity":["high","economic"],
      "symptom":{"telemetry":["transponder_channels","c_ku_band_signal"],"description":"GEO转发器通道失效/损坏,广播通信中断,业务收入损失"},
      "cause":{"environmental":["space_weather","superstorm_charging","energetic_electrons"],"internal":["aging","tt_degrades"]},
      "recovery":["spare_transponder_margin","redundancy","traffic_reto_route","fleet_redundancy"]}],
  "simulation":{
    "state_space":{"states":["operational","degraded","disabled"],"transitions":{"operational":["degraded"],"degraded":["disabled"]}},
    "models":[{"model_id":"MOD-GEO","target":"Transponder","kind":"fleet_reliability","state":["active_transponders"],"input":["age_years"],
               "parameters":{"fleet_size":300.0,"spare_ratio":0.3},"fault_injection":{"disable_frac":0.2},"output":["transponder_channels"]}],
    "sensors":["transponder_channels"],"time_resolution_ms":1000.0,"duration_s":31536000.0,
    "scenario_templates":[{"id":"SCN-GEO","fault":"F-GEO-TRANSP-NET","inject_at_s":86400,"observables":["transponder_channels"]}]},
  "rules":[{"rule_id":"R1","if":"active_transponders < 70%","then":"raise fleet redundancy concern"}],
  "tags":["GEO","通信卫星","转发器","业务中断","在轨服务"]
}
write("spacecraft-fault-012-geo.json", comm_sat)

print("done batch 3 (010-012)")
