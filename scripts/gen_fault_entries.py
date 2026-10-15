#!/usr/bin/env python3
"""Generate real-world spacecraft failure knowledge entries (format-matched to
docs/knowledge/spacecraft-fault-001.json)."""
import json, os

OUT = "docs/knowledge"

def write(name, entry):
    path = os.path.join(OUT, name)
    with open(path, "w") as f:
        json.dump(entry, f, ensure_ascii=False, indent=2)
    print("written", name)

# ---------- Common ontology scaffold (reused, matched to -001) ----------
def base_ontology():
    return {
        "namespace": "tianta:aero",
        "root_class": "SpacecraftComponent",
        "classes": [
            {"name": "AttitudeControlSystem", "parent": "SpacecraftComponent", "components": ["ReactionWheel","MomentumWheel","StarSensor","Gyroscope","FineGuidanceSensor","Magnetorquer"]},
            {"name": "ElectricalPowerSystem", "parent": "SpacecraftComponent", "components": ["SolarArray","Battery","PowerController","Bus"]},
            {"name": "GuidanceNavigationControl", "parent": "SpacecraftComponent", "components": ["InertialNavigation","ThrusterFiringModel","GroundNavigation","FlightSoftware"]},
            {"name": "TelemetryTrackingCommand", "parent": "SpacecraftComponent", "components": ["HighGainAntenna","LowGainAntenna","Transmitter","Link"]},
            {"name": "OpticalSystem", "parent": "SpacecraftComponent", "components": ["PrimaryMirror","PointSpreadFunction","Detector"]},
            {"name": "CommandAndDataHandling", "parent": "SpacecraftComponent", "components": ["OnboardComputer","CommandProcessor"]},
            {"name": "Propulsion", "parent": "SpacecraftComponent", "components": ["Thruster","PropellantTank"]},
        ],
        "instances": [],
        "properties": [
            {"name": "trajectory_altitude", "domain": "GuidanceNavigationControl", "range": "km", "observable": True},
            {"name": "angular_velocity", "domain": "AttitudeControlSystem", "range": "deg/s", "observable": True},
            {"name": "thruster_impulse", "domain": "Propulsion", "range": "Ns", "observable": True},
            {"name": "bearing_torque", "domain": "FineGuidanceSensor", "range": "Nm", "observable": True},
            {"name": "antenna_deployed_angle", "domain": "HighGainAntenna", "range": "deg", "observable": True},
            {"name": "psf_concentration", "domain": "OpticalSystem", "range": "ratio", "observable": True},
        ],
        "relations": [],
    }

# =====================================================================
# CASE 1: Mars Climate Orbiter (1999) — navigation/unit mismatch
# =====================================================================
mco = {
  "schema_version": "1.0",
  "knowledge_type": "ontology+simulation",
  "id": "SPACECRAFT-FAULT-002",
  "title": "真实案例:Mars Climate Orbiter(1999)火星气象轨道器失事",
  "domain": ["aerospace","guidance-navigation","software","unit-conversion"],
  "created": "2026-10-14",
  "provenance": {
    "source": ["AnySearch multi-domain retrieval","NASA MCO Mishap Investigation Board"],
    "references": [
      "Nature 1999 Concern over Mars Lander as inquiry reports on Orbiter loss",
      "NASA Mars Climate Orbiter Mishap Investigation Board Report",
      "Wikipedia: Mars Climate Orbiter",
      "SimScale/EverydayAstronaut metric error analyses"
    ],
    "confidence": "high",
    "mission": {"name": "Mars Climate Orbiter (MCO)", "agency": "NASA", "launch": "1998-12-11", "failure_date": "1999-09-23", "cost_mission_usd": 125e6}
  },
  "ontology": { **base_ontology(),
    "instances": [
      {"id": "INST-MCO-NAV", "class": "GuidanceNavigationControl", "role": "ground_navigation_model"},
      {"id": "INST-MCO-FSW", "class": "FlightSoftware", "role": "attitude_control"},
      {"id": "INST-MCO-THRUST", "class": "Thruster", "role": "trajectory_correction"}
    ],
    "relations": [
      {"type": "causes", "from": "unit_mismatch_si_vs_customary", "to": "trajectory_altitude_error"},
      {"type": "causes", "from": "trajectory_altitude_error", "to": "atmospheric_breakup"},
      {"type": "mitigated_by", "fault": "unit_mismatch", "action": "unified_units_ground_system"}
    ]
  },
  "fault_models": [
    {"fault_id": "F-MCO-NAV-UNIT",
     "type": ["SoftwareFailure","GroundNavigationError","UnitConversionError"],
     "component": "GuidanceNavigationControl",
     "severity": "catastrophic(mission_lost)",
     "symptom": {"telemetry": ["trajectory_altitude"], "description": "轨道器进入火星轨道的近心点过低(约57km,低于预期),最终坠入火星大气解体,遥测丢失"},
     "cause": {"internal": ["si_metric_and_us_customary_unit_mismatch","unvalidated_ground_model_input","software_requirements_defect"],
               "environmental": []},
     "recovery": ["unify_units_in_ground_nav","force_unit_validation","regression_test_nav_models","independent_review_board"],
     "mechanism": "地面站计算机模型使用英制(lbf-s)推力冲量数据,而飞行软件按公制(Ns)解释,误算轨道器位置冲量,未被人为捕获。"},
  ],
  "simulation": {
    "state_space": {"states": ["nominal","trajectory-offset","atmospheric-entry"], "transitions": {"nominal":["trajectory-offset"],"trajectory-offset":["atmospheric-entry"]}},
    "models": [
      {"model_id": "MOD-MCO-TRAJ", "target": "GuidanceNavigationControl", "kind": "orbital_propagator",
       "state": ["altitude","velocity"], "input": ["thruster_impulse"],
       "parameters": {"thruster_impulse_units": "libf-s_incorrect", "scale_factor_error": 4.45},
       "fault_injection": {"unit_scale_error_factor": 4.45, "periapsis_error_km": -90.0},
       "output": ["trajectory_altitude"]}
    ],
    "sensors": ["trajectory_altitude","velocity"],
    "time_resolution_ms": 1000.0, "duration_s": 3600.0,
    "scenario_templates": [
      {"id": "SCN-MCO-UNIT", "fault": "F-MCO-NAV-UNIT", "inject_at_s": 600, "observables": ["trajectory_altitude"]}
    ]
  },
  "rules": [
    {"rule_id": "R1", "if": "trajectory_altitude < 80km", "then": "alert atmospheric entry, trajectory error"},
    {"rule_id": "R2", "if": "nav_model_units != FSW_units", "then": "halt maneuver, force unit audit"}
  ],
  "tags": ["真实案例","Mars Climate Orbiter","单位换算","导航软件","软件缺陷","1999"]
}
write("spacecraft-fault-002-mco.json", mco)

# =====================================================================
# CASE 2: Hubble Space Telescope FGS bearing anomaly (1993/1997)
# =====================================================================
hst = {
  "schema_version": "1.0",
  "knowledge_type": "ontology+simulation",
  "id": "SPACECRAFT-FAULT-003",
  "title": "真实案例:Hubble Space Telescope 细导星敏感器(FGS)轴承异常",
  "domain": ["aerospace","attitude-control","precision-mechanism","servo-bearing"],
  "created": "2026-10-14",
  "provenance": {
    "source": ["AnySearch multi-domain retrieval","NASA GSFC FGS bearing report"],
    "references": [
      "NASA NTRS: Space Telescope Fine Guidance Sensor Bearing Anomaly (1996)",
      "The Hubble Space Telescope Servicing Mission (Cambridge 1993)",
      "Hubble Servicing Mission 3A Media Reference Guide"
    ],
    "confidence": "high",
    "mission": {"name": "Hubble Space Telescope (HST)", "agency": "NASA/ESA", "launch": "1990-04-24", "failure_observed": "1993-1997"}
  },
  "ontology": { **base_ontology(),
    "instances": [
      {"id": "INST-HST-FGS1", "class": "FineGuidanceSensor", "role": "pointing_reference"},
      {"id": "INST-HST-RSU", "class": "Gyroscope", "role": "rate_sensing"}
    ],
    "relations": [
      {"type": "causes", "from": "bearing_degradation_coarse_track", "to": "stall_torque"},
      {"type": "mitigated_by", "fault": "bearing_anomaly", "action": "reduce_coarse_track_operation"},
      {"type": "mitigated_by", "fault": "bearing_anomaly", "action": "orbital_replacement_unit_swap"}
    ]
  },
  "fault_models": [
    {"fault_id": "F-HST-FGS-BEARING",
     "type": ["MechanicalBearingFailure","PrecisionMechanismDegradation"],
     "component": "FineGuidanceSensor",
     "severity": "high(pointing capability)",
     "symptom": {"telemetry": ["bearing_torque"], "description": "FGS 伺服电机多次达到堵转(stall)扭矩,轴承磨损加速,指向保持能力下降"},
     "cause": {"internal": ["bearing_rocking_degrades_race","coarse_track_operation","servo_motor_stall"],
               "environmental": ["thermal","vacuum_lubrication_loss"]},
     "recovery": ["reduce_coarse_track_duty_in_fsw","accelerated_life_test","scheduled_fgs_replacement_sm2","orbital_replacement_unit"],
     "mechanism": "小角度摇摆运动(Coarse Track)加速轴承磨损,约2000万次循环达到饱和扭矩,与地面寿命试验一致。"},
    {"fault_id": "F-HST-GYRO",
     "type": "RateSensorFailure",
     "component": "Gyroscope",
     "severity": "high",
     "symptom": {"telemetry": ["angular_velocity","gyro_drift"], "description": "陀螺老化失效,姿态确定能力下降"},
     "cause": {"internal": ["bearing_wear"], "environmental": ["life_consumption"]},
     "recovery": ["replace_all_six_gyros_sm3a","servicing_mission"]}
  ],
  "simulation": {
    "state_space": {"states": ["nominal","degraded","stall"], "transitions": {"nominal":["degraded"],"degraded":["stall"]}},
    "models": [
      {"model_id": "MOD-HST-BRG", "target": "FineGuidanceSensor", "kind": "bearing_life_model",
       "state": ["bearing_torque","wear_cycles"], "input": ["coarse_track_duty"],
       "parameters": {"cycles_to_stall": 2e7, "stall_torque_Nm": 0.02},
       "fault_injection": {"duty_cycle": 1.0, "time_to_stall_cycles": 2e7},
       "output": ["bearing_torque"]}
    ],
    "sensors": ["bearing_torque","angular_velocity"],
    "time_resolution_ms": 1000.0, "duration_s": 100000.0,
    "scenario_templates": [
      {"id": "SCN-HST-BRG", "fault": "F-HST-FGS-BEARING", "inject_at_s": 50000, "observables": ["bearing_torque"]}
    ]
  },
  "rules": [
    {"rule_id": "R1", "if": "bearing_torque > 0.9*stall", "then": "declare FGS degraded, schedule replacement"},
    {"rule_id": "R2", "if": "wear_cycles > 1.5e7", "then": "reduce coarse track duty to extend life"}
  ],
  "tags": ["真实案例","Hubble","FGS","轴承磨损","伺服","在轨维修","SM1/SM2"]
}
write("spacecraft-fault-003-hst-fgs.json", hst)

# =====================================================================
# CASE 3: Galileo High Gain Antenna deployment failure (1991)
# =====================================================================
gal = {
  "schema_version": "1.0",
  "knowledge_type": "ontology+simulation",
  "id": "SPACECRAFT-FAULT-004",
  "title": "真实案例:Galileo 高增益天线(HGA)部署失败",
  "domain": ["aerospace","deployable-structure","antenna","mechanism"],
  "created": "2026-10-14",
  "provenance": {
    "source": ["AnySearch multi-domain retrieval","NASA NTRS Galileo HGA anomaly report"],
    "references": [
      "NASA NTRS: The Galileo high gain antenna deployment anomaly (1994)",
      "Galileo mission operations records"
    ],
    "confidence": "high",
    "mission": {"name": "Galileo", "agency": "NASA", "launch": "1989-10-18", "failure_date": "1991-04-11"}
  },
  "ontology": { **base_ontology(),
    "instances": [
      {"id": "INST-GAL-HGA", "class": "HighGainAntenna", "role": "high_rate_downlink"},
      {"id": "INST-GAL-LGA", "class": "LowGainAntenna", "role": "low_rate_fallback"}
    ],
    "relations": [
      {"type": "causes", "from": "ribs_stuck_after_restraint_release", "to": "antenna_not_fully_deployed"},
      {"type": "mitigated_by", "fault": "deployment_failure", "action": "use_low_gain_antenna"}
    ]
  },
  "fault_models": [
    {"fault_id": "F-GAL-HGA-DEPLOY",
     "type": ["DeployableMechanismFailure","AntennaDeploymentAnomaly"],
     "component": "HighGainAntenna",
     "severity": "high(data_rate degraded)",
     "symptom": {"telemetry": ["antenna_deployed_angle"], "description": "1991-04-11部署序列执行后,伞形高增益天线未能达到完全展开位置,遥测显示部分展开"},
     "cause": {"internal": ["ribs_stuck_after_restraint_release","insufficient_deployment_force","binding_members_overlap"],
               "environmental": ["launch_vibration","thermal","outgassing_stiction"]},
     "recovery": ["repeated_deployment_attempts","ground_investigation_tests","use_low_gain_antenna","data_compression_to_compensate_low_rate"],
     "mechanism": "释放约束后,天线肋片卡住未能完全展开,地面分析与试验确定可能失效机理,并制定恢复策略,改用低增益天线+数据压缩维持任务。"},
  ],
  "simulation": {
    "state_space": {"states": ["stowed","partially-deployed","fully-deployed"], "transitions": {"stowed":["partially-deployed"],"partially-deployed":["fully-deployed"]}},
    "models": [
      {"model_id": "MOD-GAL-HGA", "target": "HighGainAntenna", "kind": "deployment_kinetics",
       "state": ["antenna_deployed_angle"], "input": ["deployment_command"],
       "parameters": {"deployment_force_N": 100.0, "stiction_N": 150.0},
       "fault_injection": {"binding_force_factor": 1.5, "stuck_angle_deg": 15.0},
       "output": ["antenna_deployed_angle"]}
    ],
    "sensors": ["antenna_deployed_angle"],
    "time_resolution_ms": 100.0, "duration_s": 1200.0,
    "scenario_templates": [
      {"id": "SCN-GAL-HGA", "fault": "F-GAL-HGA-DEPLOY", "inject_at_s": 300, "observables": ["antenna_deployed_angle"]}
    ]
  },
  "rules": [
    {"rule_id": "R1", "if": "antenna_deployed_angle < 100deg after 600s", "then": "declare deployment failure, switch to LGA"},
    {"rule_id": "R2", "if": "stiction > deployment_force", "then": "no retry, use fallback link"}
  ],
  "tags": ["真实案例","Galileo","高增益天线","可展开机构","部署失败","低增益天线","1991"]
}
write("spacecraft-fault-004-galileo-hga.json", gal)

print("done 3 entries")
