EQUIPMENT_FLEET = []

# Fleet layout: (type_name, prefix, total_count, bev_count)
fleet_specs = [
    ("Roof_Bolter", "BOLT", 4, 2),
    ("Cable_Bolter", "CABLE", 3, 1),
    ("Face_Drill_Jumbo", "JUMBO", 5, 2),
    ("Longhole_Drill", "LH-DRILL", 4, 2),
    ("Scooptram_LHD", "SCOOP", 6, 3),
    ("Mine_Truck_Hauler", "TRUCK", 5, 2),
    ("Anfoloader_Emulsion", "ANFO", 3, 1),
    ("Scissor_Lift", "LIFT", 4, 2),
    ("Scaler", "SCALE", 3, 1),
    ("Underground_Grader", "GRADE", 3, 1)
]

for type_name, prefix, count, bev_count in fleet_specs:
    for i in range(1, count + 1):
        is_bev = i <= bev_count
        EQUIPMENT_FLEET.append({
            "equipment_id": len(EQUIPMENT_FLEET) + 1,
            "equipment_serial_number": f"{prefix}-{i:03d}",
            "type_name": type_name,
            "category": "BEV" if is_bev else "Diesel"
        })

SIGNAL_REGISTRY = {
    # -------------------------------------------------------------------------
    # UNIVERSAL CORE TAGS (All 42 Machines - 30 Total Signals)
    # -------------------------------------------------------------------------
    "UNIVERSAL_CORE": {
        # Original 10 Core Signals
        "NS=2;s=Fuel_Or_Charge_Percent": ("PCT", "Percentage", "Powertrain", {
            "IDLE": (15, 100), "OPERATIONAL": (10, 100), "FAULT": (5, 15)
        }),
        "NS=2;s=Is_Idle": ("BOOL", "Boolean Flag", "Status", {
            "IDLE": (1, 1), "OPERATIONAL": (0, 0), "FAULT": (0, 0)
        }),
        "NS=2;s=Is_Operational": ("BOOL", "Boolean Flag", "Status", {
            "IDLE": (0, 0), "OPERATIONAL": (1, 1), "FAULT": (0, 0)
        }),
        "NS=2;s=Is_In_Fault": ("BOOL", "Boolean Flag", "Status", {
            "IDLE": (0, 0), "OPERATIONAL": (0, 0), "FAULT": (1, 1)
        }),
        "NS=2;s=Vehicle_Speed": ("KMH", "Kilometers Per Hour", "Mobility", {
            "IDLE": (0, 0), "OPERATIONAL": (5, 25), "FAULT": (0, 0)
        }),
        "NS=2;s=Location_Handshake_Zone": ("CODE", "Location Zone", "Location", {
            "IDLE": ("RAMP_PORTAL", "LEVEL_2600_SHOP"), 
            "OPERATIONAL": ("STOPE_402_HEADING", "LEVEL_2600_ORE_PASS"), 
            "FAULT": ("LEVEL_2600_RAMP_NORTH", "REFUGE_STATION_B")
        }),
        "NS=2;s=Tire_Pressure_FL": ("PSI", "Pounds per Sq Inch", "Tires", {
            "IDLE": (90, 105), "OPERATIONAL": (95, 110), "FAULT": (40, 70)
        }),
        "NS=2;s=Tire_Pressure_FR": ("PSI", "Pounds per Sq Inch", "Tires", {
            "IDLE": (90, 105), "OPERATIONAL": (95, 110), "FAULT": (40, 70)
        }),
        "NS=2;s=Tire_Pressure_RL": ("PSI", "Pounds per Sq Inch", "Tires", {
            "IDLE": (90, 105), "OPERATIONAL": (95, 110), "FAULT": (40, 70)
        }),
        "NS=2;s=Tire_Pressure_RR": ("PSI", "Pounds per Sq Inch", "Tires", {
            "IDLE": (90, 105), "OPERATIONAL": (95, 110), "FAULT": (40, 70)
        }),

        # 20 Approved Universal Telemetry Additions
        "NS=2;s=Engine_Cumulative_Hours": ("HRS", "Hours", "Powertrain", {
            "IDLE": (1200.0, 4500.0), "OPERATIONAL": (1200.0, 4500.0), "FAULT": (1200.0, 4500.0)
        }),
        "NS=2;s=Engine_Oil_Temp": ("DEGC", "Degrees Celsius", "Powertrain", {
            "IDLE": (50, 70), "OPERATIONAL": (85, 105), "FAULT": (115, 135)
        }),
        "NS=2;s=Engine_Oil_Pressure": ("PSI", "Pounds per Sq Inch", "Powertrain", {
            "IDLE": (25, 35), "OPERATIONAL": (50, 75), "FAULT": (10, 18)
        }),
        "NS=2;s=Fuel_Consumption_Rate": ("LPH", "Liters Per Hour", "Powertrain", {
            "IDLE": (3.5, 6.0), "OPERATIONAL": (18.0, 45.0), "FAULT": (0.0, 2.0)
        }),
        "NS=2;s=Transmission_Oil_Temp": ("DEGC", "Degrees Celsius", "Powertrain", {
            "IDLE": (40, 55), "OPERATIONAL": (70, 95), "FAULT": (105, 125)
        }),
        "NS=2;s=Transmission_Oil_Pressure": ("PSI", "Pounds per Sq Inch", "Powertrain", {
            "IDLE": (120, 150), "OPERATIONAL": (220, 280), "FAULT": (50, 90)
        }),
        "NS=2;s=Brake_Cooling_Oil_Temp": ("DEGC", "Degrees Celsius", "Braking", {
            "IDLE": (30, 45), "OPERATIONAL": (60, 85), "FAULT": (95, 115)
        }),
        "NS=2;s=Hydraulic_Filter_Restriction_Delta": ("PSI", "Pounds per Sq Inch", "Hydraulics", {
            "IDLE": (2, 5), "OPERATIONAL": (8, 18), "FAULT": (25, 40)
        }),
        "NS=2;s=Hydraulic_Oil_Level_Percent": ("PCT", "Percentage", "Hydraulics", {
            "IDLE": (85, 100), "OPERATIONAL": (75, 95), "FAULT": (30, 50)
        }),
        "NS=2;s=12V_Control_Bus_Voltage": ("VOLTS", "Volts DC", "Electrical", {
            "IDLE": (13.2, 13.8), "OPERATIONAL": (13.5, 14.2), "FAULT": (10.5, 11.8)
        }),
        "NS=2;s=Exhaust_Gas_Temp": ("DEGC", "Degrees Celsius", "Powertrain", {
            "IDLE": (150, 220), "OPERATIONAL": (350, 520), "FAULT": (600, 750)
        }),
        "NS=2;s=Coolant_Level_Percent": ("PCT", "Percentage", "Powertrain", {
            "IDLE": (85, 100), "OPERATIONAL": (80, 95), "FAULT": (20, 45)
        }),
        "NS=2;s=Cabin_Operator_Seat_Occupied": ("BOOL", "Boolean Flag", "Safety", {
            "IDLE": (0, 1), "OPERATIONAL": (1, 1), "FAULT": (0, 1)
        }),
        "NS=2;s=Fire_Suppression_System_Pressure": ("PSI", "Pounds per Sq Inch", "Safety", {
            "IDLE": (340, 360), "OPERATIONAL": (340, 360), "FAULT": (100, 200)
        }),
        "NS=2;s=Articulated_Frame_Steer_Angle": ("DEGREES", "Degrees", "Steering", {
            "IDLE": (0, 0), "OPERATIONAL": (-35, 35), "FAULT": (0, 0)
        }),
        "NS=2;s=Parking_Brake_Engaged": ("BOOL", "Boolean Flag", "Braking", {
            "IDLE": (1, 1), "OPERATIONAL": (0, 0), "FAULT": (1, 1)
        }),
        "NS=2;s=Driveshaft_RPM": ("RPM", "Revolutions Per Minute", "Powertrain", {
            "IDLE": (0, 0), "OPERATIONAL": (400, 1800), "FAULT": (0, 0)
        }),
        "NS=2;s=Ambient_Mine_Air_Temp": ("DEGC", "Degrees Celsius", "Environment", {
            "IDLE": (18, 24), "OPERATIONAL": (20, 28), "FAULT": (20, 28)
        }),
        "NS=2;s=System_Uptime_Seconds": ("SEC", "Seconds", "Diagnostics", {
            "IDLE": (3600, 86400), "OPERATIONAL": (3600, 86400), "FAULT": (3600, 86400)
        }),
        "NS=2;s=Emergency_Stop_Circuit_Status": ("BOOL", "Boolean Flag", "Safety", {
            "IDLE": (1, 1), "OPERATIONAL": (1, 1), "FAULT": (0, 0)
        })
    },

    # -------------------------------------------------------------------------
    # GROUND SUPPORT (Bolters)
    # -------------------------------------------------------------------------
    "Roof_Bolter": {
        "NS=2;s=Resin_Container_A_Level_Percent": ("PCT", "Percentage", "Ground_Support", {
            "IDLE": (20, 100), "OPERATIONAL": (10, 100), "FAULT": (0, 5)
        }),
        "NS=2;s=Resin_Container_B_Level_Percent": ("PCT", "Percentage", "Ground_Support", {
            "IDLE": (20, 100), "OPERATIONAL": (10, 100), "FAULT": (0, 5)
        }),
        "NS=2;s=Bolts_Installed_Count": ("COUNT", "Discrete Count", "Ground_Support", {
            "IDLE": (0, 0), "OPERATIONAL": (1, 1), "FAULT": (0, 0)
        }),
        "NS=2;s=Holes_Drilled_Count": ("COUNT", "Discrete Count", "Ground_Support", {
            "IDLE": (0, 0), "OPERATIONAL": (1, 1), "FAULT": (0, 0)
        }),
        "NS=2;s=Hole_Meters_Drilled": ("METERS", "Meters", "Ground_Support", {
            "IDLE": (0.0, 0.0), "OPERATIONAL": (2.4, 3.0), "FAULT": (0.0, 0.0)
        }),
        "NS=2;s=Holes_Cleaned_Count": ("COUNT", "Discrete Count", "Ground_Support", {
            "IDLE": (0, 0), "OPERATIONAL": (1, 1), "FAULT": (0, 0)
        }),
        "NS=2;s=Grout_Pump_Pressure": ("PSI", "Pounds per Sq Inch", "Ground_Support", {
            "IDLE": (0, 50), "OPERATIONAL": (1200, 1600), "FAULT": (2100, 2500)
        })
    },
    "Cable_Bolter": {
        "NS=2;s=Resin_Container_A_Level_Percent": ("PCT", "Percentage", "Ground_Support", {
            "IDLE": (20, 100), "OPERATIONAL": (10, 100), "FAULT": (0, 5)
        }),
        "NS=2;s=Resin_Container_B_Level_Percent": ("PCT", "Percentage", "Ground_Support", {
            "IDLE": (20, 100), "OPERATIONAL": (10, 100), "FAULT": (0, 5)
        }),
        "NS=2;s=Cable_Feed_Length": ("METERS", "Meters", "Ground_Support", {
            "IDLE": (0.0, 0.0), "OPERATIONAL": (10.0, 25.0), "FAULT": (0.0, 0.0)
        }),
        "NS=2;s=Holes_Drilled_Count": ("COUNT", "Discrete Count", "Ground_Support", {
            "IDLE": (0, 0), "OPERATIONAL": (1, 1), "FAULT": (0, 0)
        }),
        "NS=2;s=Hole_Meters_Drilled": ("METERS", "Meters", "Ground_Support", {
            "IDLE": (0.0, 0.0), "OPERATIONAL": (10.0, 25.0), "FAULT": (0.0, 0.0)
        }),
        "NS=2;s=Holes_Cleaned_Count": ("COUNT", "Discrete Count", "Ground_Support", {
            "IDLE": (0, 0), "OPERATIONAL": (1, 1), "FAULT": (0, 0)
        })
    },

    # -------------------------------------------------------------------------
    # DRILLING (Jumbos & Longholes)
    # -------------------------------------------------------------------------
    "Face_Drill_Jumbo": {
        "NS=2;s=Holes_Drilled_Count": ("COUNT", "Discrete Count", "Drilling", {
            "IDLE": (0, 0), "OPERATIONAL": (1, 1), "FAULT": (0, 0)
        }),
        "NS=2;s=Hole_Meters_Drilled": ("METERS", "Meters", "Drilling", {
            "IDLE": (0.0, 0.0), "OPERATIONAL": (3.5, 4.5), "FAULT": (0.0, 0.0)
        }),
        "NS=2;s=Holes_Cleaned_Count": ("COUNT", "Discrete Count", "Drilling", {
            "IDLE": (0, 0), "OPERATIONAL": (1, 1), "FAULT": (0, 0)
        }),
        "NS=2;s=Drill_Feed_Pressure": ("PSI", "Pounds per Sq Inch", "Drilling", {
            "IDLE": (0, 100), "OPERATIONAL": (1800, 2400), "FAULT": (2800, 3200)
        }),
        "NS=2;s=Water_Mist_Pressure": ("PSI", "Pounds per Sq Inch", "Drilling", {
            "IDLE": (0, 50), "OPERATIONAL": (300, 500), "FAULT": (10, 40)
        })
    },
    "Longhole_Drill": {
        "NS=2;s=Holes_Drilled_Count": ("COUNT", "Discrete Count", "Drilling", {
            "IDLE": (0, 0), "OPERATIONAL": (1, 1), "FAULT": (0, 0)
        }),
        "NS=2;s=Hole_Meters_Drilled": ("METERS", "Meters", "Drilling", {
            "IDLE": (0.0, 0.0), "OPERATIONAL": (15.0, 30.0), "FAULT": (0.0, 0.0)
        }),
        "NS=2;s=Holes_Cleaned_Count": ("COUNT", "Discrete Count", "Drilling", {
            "IDLE": (0, 0), "OPERATIONAL": (1, 1), "FAULT": (0, 0)
        }),
        "NS=2;s=Percussion_Hours": ("HRS", "Hours", "Drilling", {
            "IDLE": (0.0, 0.0), "OPERATIONAL": (0.1, 1.0), "FAULT": (0.0, 0.0)
        })
    },

    # -------------------------------------------------------------------------
    # MATERIAL HANDLING & HAULAGE (Scoops & Trucks)
    # -------------------------------------------------------------------------
    "Scooptram_LHD": {
        "NS=2;s=Bucket_Pass_Count": ("COUNT", "Discrete Count", "Material_Handling", {
            "IDLE": (0, 0), "OPERATIONAL": (1, 1), "FAULT": (0, 0)
        }),
        "NS=2;s=Payload_Tonnage": ("TONNES", "Metric Tonnes", "Material_Handling", {
            "IDLE": (0.0, 0.5), "OPERATIONAL": (10.0, 18.0), "FAULT": (20.0, 24.0)
        }),
        "NS=2;s=Hydraulic_Lift_Pressure": ("PSI", "Pounds per Sq Inch", "Hydraulics", {
            "IDLE": (200, 400), "OPERATIONAL": (2200, 3000), "FAULT": (3400, 3800)
        })
    },
    "Mine_Truck_Hauler": {
        "NS=2;s=Payload_Tonnage": ("TONNES", "Metric Tonnes", "Material_Handling", {
            "IDLE": (0.0, 1.0), "OPERATIONAL": (30.0, 60.0), "FAULT": (65.0, 75.0)
        }),
        "NS=2;s=Dump_Bed_Incline_Angle": ("DEGREES", "Degrees", "Material_Handling", {
            "IDLE": (0, 0), "OPERATIONAL": (0, 45), "FAULT": (0, 0)
        })
    },

    # -------------------------------------------------------------------------
    # EXPLOSIVES, SERVICES & UTILITIES
    # -------------------------------------------------------------------------
    "Anfoloader_Emulsion": {
        "NS=2;s=Emulsion_Pumping_Pressure": ("PSI", "Pounds per Sq Inch", "Explosives", {
            "IDLE": (0, 20), "OPERATIONAL": (200, 450), "FAULT": (550, 700)
        }),
        "NS=2;s=Hose_Reel_Encoder_Count": ("COUNT", "Discrete Count", "Explosives", {
            "IDLE": (0, 0), "OPERATIONAL": (1, 100), "FAULT": (0, 0)
        })
    },
    "Scissor_Lift": {
        "NS=2;s=Platform_Height_Meters": ("METERS", "Meters", "Utility", {
            "IDLE": (0.0, 0.5), "OPERATIONAL": (1.5, 4.5), "FAULT": (0.0, 0.0)
        })
    },
    "Scaler": {
        "NS=2;s=Scaler_Hammer_Impact_Freq": ("HZ", "Hertz", "Scaling", {
            "IDLE": (0, 0), "OPERATIONAL": (15, 35), "FAULT": (45, 60)
        })
    },
    "Underground_Grader": {
        "NS=2;s=Grader_Blade_Position_MM": ("MM", "Millimeters", "Road_Maintenance", {
            "IDLE": (0, 0), "OPERATIONAL": (-50, 150), "FAULT": (0, 0)
        })
    }
}