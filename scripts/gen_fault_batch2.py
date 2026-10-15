#!/usr/bin/env python3
"""Generate additional real-world + generic spacecraft fault entries (005+),
matching the schema of spacecraft-fault-001.json (ontology + simulation)."""
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
            {"name": "AttitudeControlSystem", "parent": "SpacecraftComponent", "components": ["ReactionWheel","MomentumWheel","StarSensor","Gyroscope","FineGuidanceSensor","Magnetorquer"]},
            {"name": "ElectricalPowerSystem", "parent": "SpacecraftComponent", "components": ["SolarArray","Battery","PowerController","Bus"]},
            {"name": "ThermalControlSystem", "parent": "SpacecraftComponent", "components": ["Thermistor","Heater","Radiator","OSR"]},
            {"name": "TelemetryTrackingCommand", "parent": "SpacecraftComponent", "components": ["Transponder","Transmitter","HighGainAntenna","Antenna"]},
            {"name": "Payload", "parent": "SpacecraftComponent", "components": ["Instrument","ScienceDataLink"]},
            {"name": "CommandAndDataHandling", "parent": "SpacecraftComponent", "components": ["OnboardComputer","CommandProcessor","FlightSoftware","Memory"]},
            {"name": "Propulsion", "parent": "SpacecraftComponent", "components": ["Thruster","HallThruster","PropellantTank","Valve"]},
            {"name": "GuidanceNavigationControl", "parent": "SpacecraftComponent", "components": ["InertialReferenceSystem","InertialNavigation","GroundNavigation"]},
        ],
        "instances": [],
        "properties": [],
        "relations": [],
    }

# =====================================================================
# 005 — Propulsion/RCS 推进子系统故障 (generic + Hall thruster)
# =====================================================================
gen_prop = {
  "schema_version": "1.0", "knowledge_type": "ontology+simulation",
  "id": "SPACECRAFT-FAULT-005",
  "title": "推进子系统故障(通用 + 电推进)",
  "domain": ["aerospace","propulsion","thruster"],
  "created": "2026-10-14",
  "provenance": {"source":["AnySearch retrieval"],
    "references":["Acta Astronautica 2019 station-keeping with electric propulsion failure modes",
                   "Electric propulsion for small satellites (IOP)," "Hall Thruster on Express (2007)"],
    "confidence":"high"},
  "ontology":{**base_ontology(),
    "instances":[{"id":"INST-THRUST1","class":"Thruster","role":"station_keeping"}],
    "relations":[{"type":"causes","from":"thruster_impulse_failure","to":"orbit_error"}]},
  "fault_models":[
    {"fault_id":"F-PROP-THRUSTER","type":["PropulsionFailure","ThrusterFailure"],
      "component":"Thruster","severity":"high","symptom":{"telemetry":["thruster_impulse","orbit"],"description":"推力/冲量异常,轨道保持失败,推进剂压力异常"},
      "cause":{"internal":["valve_failure","thruster_wear","propellant_tank_pressure"],"environmental":[]},
      "recovery":["redundant_thruster_switch","propellant_pressure_manage","station_keeping_replan"]},
    {"fault_id":"F-PROP-HALL","type":"ElectricPropulsionFailure",
      "component":"HallThruster","severity":"medium","symptom":{"telemetry":["ion_current","thrust"],"description":"电推进羽流/离子电流异常,推力下降"},
      "cause":{"internal":["cathode_degradation"],"environmental":[]},
      "recovery":["thruster_switch","reduced_thrust_mode"]}],
  "simulation":{
    "state_space":{"states":["nominal","degraded","failed"],"transitions":{"nominal":["degraded"],"degraded":["failed"]}},
    "models":[{"model_id":"MOD-PROP","target":"Thruster","kind":"impulse_model","state":["orbital_velocity"],"input":["thruster_command"],
               "parameters":{"thrust_N":0.05,"isp":300},"fault_injection":{"thrust_factor":0.5},"output":["thruster_impulse"]}],
    "sensors":["thruster_impulse","ion_current"],"time_resolution_ms":100.0,"duration_s":3600.0,
    "scenario_templates":[{"id":"SCN-PROP","fault":"F-PROP-THRUSTER","inject_at_s":600,"observables":["thruster_impulse"]}]},
  "rules":[{"rule_id":"R1","if":"thruster_impulse < 50% nominal","then":"switch redundant thruster"}],
  "tags":["推进","电推进","霍尔推力器","推力器故障","轨道保持"]
}
write("spacecraft-fault-005-propulsion.json", gen_prop)

# =====================================================================
# 006 — Solar Array 太阳翼/供电故障 (generic)
# =====================================================================
gen_solar = {
  "schema_version": "1.0","knowledge_type":"ontology+simulation",
  "id":"SPACECRAFT-FAULT-006",
  "title":"太阳翼/供电子系统故障",
  "domain":["aerospace","power","solar-array"],
  "created":"2026-10-14",
  "provenance":{"source":["AnySearch retrieval"],
    "references":["NASA CubeSat solar array deployment study","XTE solar array deployment & drive system (GSFC)","Flexible multibody deployment (ABAQUS)"],
    "confidence":"high"},
  "ontology":{**base_ontology(),
    "instances":[{"id":"INST-SA1","class":"SolarArray","role":"power_generation"}],
    "relations":[{"type":"causes","from":"deployment_failure","to":"power_shortfall"}]},
  "fault_models":[
    {"fault_id":"F-PWR-SOLAR-DEPLOY","type":["PowerFailure","DeploymentFailure"],
      "component":"SolarArray","severity":["high","common_cubeSat_failure"],
      "symptom":{"telemetry":["solar_array_power","deployed_angle"],"description":"太阳翼未完全展开,发电功率不足(小卫星最常见故障原因)"},
      "cause":{"internal":["hinge_failure","boom_stuck","damping_issue"],"environmental":["launch_vibration","thermal"]},
      "recovery":["retry_deployment","manual_swing","power_management","reduced_load"]},
    {"fault_id":"F-PWR-SOLAR-DEGRADE","type":"PowerDegradation",
      "component":"SolarArray","severity":"medium","symptom":{"telemetry":["solar_array_power"],"description":"辐照损伤/温度致功率缓慢下降"},
      "cause":{"internal":["radiation_damage","thermal"],"environmental":["radiation"]},
      "recovery":["power_reallocate","derating"]}],
  "simulation":{
    "state_space":{"states":["stowed","partial","deployed","degraded"],"transitions":{"stowed":["partial","deployed"],"partial":["deployed"],"deployed":["degraded"]}},
    "models":[{"model_id":"MOD-SOLAR","target":"SolarArray","kind":"deployment+power","state":["deployed_angle","power_out"],"input":["deployment_command"],
               "parameters":{"deployment_force_N":50.0,"cell_efficiency":0.3},"fault_injection":{"stuck_angle_deg":10.0,"power_factor":0.1},"output":["solar_array_power"]}],
    "sensors":["solar_array_power","deployed_angle"],"time_resolution_ms":100.0,"duration_s":3600.0,
    "scenario_templates":[{"id":"SCN-SOLAR","fault":"F-PWR-SOLAR-DEPLOY","inject_at_s":300,"observables":["solar_array_power"]}]},
  "rules":[{"rule_id":"R1","if":"solar_array_power < 20% expected","then":"declare deployment failure, power manage"}],
  "tags":["太阳翼","供电","部署失败","发电","小卫星"]
}
write("spacecraft-fault-006-solar.json", gen_solar)

# =====================================================================
# 007 — Radiation / SEU 空间环境故障
# =====================================================================
gen_rad = {
  "schema_version": "1.0","knowledge_type":"ontology+simulation",
  "id":"SPACECRAFT-FAULT-007",
  "title":"空间环境故障:辐射/单粒子翻转/充电",
  "domain":["aerospace","space-environment","radiation","SEU"],
  "created":"2026-10-14",
  "provenance":{"source":["AnySearch retrieval"],
    "references":["SEU rate estimates 16-K CMOS SRAM (IEEE TNS 1985)","Beidou SEU/LET monitor","Mega satellite constellation under SEU (IEEE TCOMM)","BRAM SEU sensor (IEEE)","1859 superstorm transponder impact"],
    "confidence":"high"},
  "ontology":{**base_ontology(),
    "instances":[{"id":"INST-MEM","class":"Memory","role":"flight_memory"}],
    "relations":[{"type":"causes","from":"SEU","to":"data_corruption"},{"type":"causes","from":"charging","to":"electrostatic_discharge"}]},
  "fault_models":[
    {"fault_id":"F-ENV-SEU","type":"RadiationEffect","component":"Memory","severity":"medium",
      "symptom":{"telemetry":["memory_error","reboot_count"],"description":"单粒子翻转导致内存数据错误、通信组件异常、星载计算机需纠错/重启"},
      "cause":{"environmental":["heavy_ion","solar_proton","cosmic_ray"],"internal":["critical_charge_low"]},
      "recovery":["error_correction_edac","scrubbing","watchdog_reboot","memory_reloaded"]},
    {"fault_id":"F-ENV-CHARGING","type":"ElectrostaticEffect","component":"Bus","severity":["high","damaging"],
      "symptom":{"telemetry":["surge","efield"],"description":"表面/深充导致静电放电,损伤电子器件"},
      "cause":{"environmental":["energetic_electron","geo_magnetic"],"internal":[]},
      "recovery":["grounded_surface","discharge_mitigation","sensitive_gPU_protection"]}],
  "simulation":{
    "state_space":{"states":["nominal","SEE","latchup"],"transitions":{"nominal":["SEE"],"SEE":["latchup"]}},
    "models":[{"model_id":"MOD-SEU","target":"Memory","kind":"upset_rate_model","state":["error_count"],"input":["radiation_flux"],
               "parameters":{"critical_charge_pC":0.01,"upset_rate_per_day":0.5},"fault_injection":{"upt_set_rate_x10":10.0},"output":["memory_error"]}],
    "sensors":["memory_error","reboot_count"],"time_resolution_ms":1000.0,"duration_s":86400.0,
    "scenario_templates":[{"id":"SCN-SEU","fault":"F-ENV-SEU","inject_at_s":3600,"observables":["memory_error"]}]},
  "rules":[{"rule_id":"R1","if":"memory_error_rate > threshold","then":"trigger EDAC+scrubbing"},
           {"rule_id":"R2","if":"surge detected","then":"initiate discharge mitigation"}],
  "tags":["辐射","单粒子翻转","SEU","静电","空间环境","纠错"]
}
write("spacecraft-fault-007-radiation.json", gen_rad)

# =====================================================================
# 008 — Ariane 5 Flight 501 (real case)
# =====================================================================
ari = {
  "schema_version": "1.0","knowledge_type":"ontology+simulation",
  "id":"SPACECRAFT-FAULT-008",
  "title":"真实案例:Ariane 5 Flight 501(1996)火箭制导失效",
  "domain":["aerospace","launcher","software","inertial-navigation"],
  "created":"2026-10-14",
  "provenance":{"source":["AnySearch retrieval"],
    "references":["ESA Ariane 501 Inquiry Board report","Ariane 5 Flight 501 analysis","Ariane flight V88 reused-code failure"],
    "confidence":"high",
    "mission":{"name":"Ariane 5 Flight 501","company":"ESA/CNES/Arianespace","failure_date":"1996-06-04"}},
  "ontology":{**base_ontology(),
    "instances":[{"id":"INST-IRS1","class":"InertialReferenceSystem","role":"guidance"}],
    "relations":[{"type":"causes","from":"irs_software_horizontal_velocity_overflow","to":"loss_guidance_attitude"},{"type":"causes","from":"loss_guidance","to":"launch_deviated"}]},
  "fault_models":[
    {"fault_id":"F-ARI501-IRS-SW","type":["SoftwareFailure","GuidanceFailure"],
      "component":"InertialReferenceSystem","severity":"catastrophic(launch_failure)",
      "symptom":{"telemetry":["guidance_data"],"description":"主发动机点火后37秒失去全部制导与姿态信息(升空30秒),运载火箭偏离,自毁"},
      "cause":{"internal":["horizontal_velocity_64bit_overflow_in_ada","reused_flight_code_without_revalidation","unnecessary_routine_running_during_flight"]},
      "recovery":["disable_alignment_after_lift_off","no_unneeded_sw_during_flight","revalidate_reused_code","inquiry_board"]},
  ],
  "simulation":{
    "state_space":{"states":["ok","overflow","loss_guidance"],"transitions":{"ok":["overflow"],"overflow":["loss_guidance"]}},
    "models":[{"model_id":"MOD-ARI-IRS","target":"InertialReferenceSystem","kind":"velocity_overflow","state":["horizontal_velocity"],"input":["time"],
               "parameters":{"overflow_threshold":65535.0,"t_overflow_s":37},"fault_injection":{"velocity_factor":1.0},"output":["guidance_data"]}],
    "sensors":["guidance_data"],"time_resolution_ms":100.0,"duration_s":120.0,
    "scenario_templates":[{"id":"SCN-ARI501","fault":"F-ARI501-IRS-SW","inject_at_s":37,"observables":["guidance_data"]}]},
  "rules":[{"rule_id":"R1","if":"velocity > overflow_threshold","then":"flag overflow, guidance degraded"}],
  "tags":["真实案例","Ariane5","501","软件缺陷","惯导","1996","整型溢出"]
}
write("spacecraft-fault-008-ariane501.json", ari)

# =====================================================================
# 009 — Apollo 13 (real case)
# =====================================================================
apollo = {
  "schema_version": "1.0","knowledge_type":"ontology+simulation",
  "id":"SPACECRAFT-FAULT-009",
  "title":"真实案例:Apollo 13(1970)氧气罐爆炸",
  "domain":["aerospace","crewed-mission","cryogenic","electrical"],
  "created":"2026-10-14",
  "provenance":{"source":["AnySearch retrieval"],
    "references":["NASA Apollo 13 Review Board Report (TMX-65270)","Apollo Lesson Sampler: Apollo 13","Apollo 13 Mission Report"],
    "confidence":"high",
    "mission":{"name":"Apollo 13","agency":"NASA","failure_date":"1970-04-13"}},
  "ontology":{**base_ontology(),
    "instances":[{"id":"INST-O2TANK2","class":"PropellantTank","role":"cryogenic_oxygen"}],
    "relations":[{"type":"causes","from":"tank_fire","to":"loss_oxygen_power"}]},
  "fault_models":[
    {"fault_id":"F-APOLLO13-O2TANK","type":["CryogenicFailure","FireExplosion"],
      "component":"PropellantTank","severity":"catastrophic(mission abort)",
      "symptom":{"telemetry":["tank_pressure","power","oxygen"],"description":"约56小时服务舱氧气罐#2起火爆炸,失去氧气与电力,任务中止,利用登月舱氧气/电力返回"},
      "cause":{"internal":["tank_overheating_from_heater","manuf_tank_processing_history","teflon_wiring_insulation_under_heated_tank"],
               "environmental":[]},
      "recovery":["lunar_module_as_lifeboat","power_off_nonessential","manual_trajectory","successful_entry"]},
  ],
  "simulation":{
    "state_space":{"states":["ok","overheat","fire"],"transitions":{"ok":["overheat"],"overheat":["fire"]}},
    "models":[{"model_id":"MOD-APOLLO-O2","target":"PropellantTank","kind":"pressure_thermal","state":["tank_pressure","tank_temp"],"input":["heater_power"],
               "parameters":{"critical_pressure_psi":1000.0},"fault_injection":{"heater_stuck_on":True,"ignition_temp_K":500.0},"output":["tank_pressure"]}],
    "sensors":["tank_pressure","power","oxygen"],"time_resolution_ms":1000.0,"duration_s":86400.0,
    "scenario_templates":[{"id":"SCN-APOLLO13","fault":"F-APOLLO13-O2TANK","inject_at_s":300,"observables":["tank_pressure"]}]},
  "rules":[{"rule_id":"R1","if":"tank_temp > ignition_temp","then":"emergency, isolate tank"},{"rule_id":"R2","if":"oxygen lost","then":"switch to lunar module lifeboat"}],
  "tags":["真实案例","Apollo13","氧气罐","低温","爆炸","1970","载人"]
}
write("spacecraft-fault-009-apollo13.json", apollo)

print("done batch 005-009")
