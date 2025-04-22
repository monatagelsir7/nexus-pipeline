"""
Configuration module for data processing pipeline.
Contains configuration dictionaries for various data sources.
"""

# IMF ISORA file configuration
ISORA_FILES = {
    "imf_isora_resources_ict": {
        "filepath": "imf isora resources and ICT infrastructure.xlsx",
        "sheets": {
            "Tax administration expenditures": {"start": 6, "end": 172},
            # trailing space error in sheetname
            "Tax administration staff total ": {"start": 7, "end": 173},
            "Operational ICT solutions": {"start": 6, "end": 172}
        }
    },
    "imf_isora_staff_metrics": {
        "filepath": "imf isora staff metrics.xlsx",
        "sheets": {
            "Staff strength levels": {"start": 6, "end": 172},
            "Staff academic qualifications": {"start": 6, "end": 172},
            "Staff age distribution": {"start": 6, "end": 172},
            "Staff length of service": {"start": 6, "end": 172},
            "Staff gender distribution": {"start": 7, "end": 172}
        }
    },
    "imf_isora_op_metrics_audit": {
        "filepath": "imf isora op metrics audit, criminal investigations, dispute resolution.xlsx",
        "sheets": {
            "Audit and verification": {"start": 6, "end": 172},
            "Value of additional assessments": {"start": 6, "end": 172},
            "Value of additional assessm_0": {"start": 6, "end": 172},
            "Tax crime investigation": {"start": 6, "end": 172},
            "Dispute resolution review proce": {"start": 6, "end": 172}
        }
    },
    "imf_isora": {
        "filepath": "IMF ISORA.xlsx",
        "sheets": {
            "Segmentation ratios LTO or prog": {"start": 5, "end": 171},
            "Registration of personal income": {"start": 5, "end": 171},
            "Percentage inactive taxpayers o": {"start": 5, "end": 171},
            "On-time filing rates by tax typ": {"start": 5, "end": 171},
            # trailing space error in sheetname
            "Electronic filing rates by tax ": {"start": 5, "end": 171},
            "Proportion of returns by channe": {"start": 5, "end": 171},
            "Proportion of returns by ch_0": {"start": 5, "end": 171},
            "Proportion of returns by ch_1": {"start": 5, "end": 171}
        }
    }
}

# World Development Indicators codes
WDI_INDICATOR_CODES = [
    "FS.AST.DOMS.GD.ZS",
    "FB.BNK.CAPA.ZS",
    "FD.RES.LIQU.AS.ZS",
    "GC.TAX.TOTL.GD.ZS",
    "IQ.CPA.PUBS.XQ",
    "IQ.CPA.PADM.XQ",
    "FX.OWN.TOTL.YG.ZS",
    "FX.OWN.TOTL.OL.ZS",
    "CM.MKT.LCAP.CD",
    "NY.GDP.MKTP.CD",
    "DT.NFL.BOND.CD",
    "BN.RES.INCL.CD",
    "DT.DOD.DSTC.CD",
    "CC.EST"
]

# World Governance Indicators labels
WGI_INDICATOR_LABELS = {
    'va': 'Voice and Accountability',
    'pv': 'Political Stability and Absence of Violence/Terrorism',
    'ge': 'Government Effectiveness',
    'rq': 'Regulatory Quality',
    'rl': 'Rule of Law',
    'cc': 'Control of Corruption'
}

# Global Financial Integrity configuration
GFI_CONFIG = {
    "Table A": {
        "indicator_label": "The Sums of the Value Gaps Identified in Trade Between 134 Developing Countries and 36 Advanced Economies, 2009–2018, in USD Millions",
        "indicator_code": "GFI.TableA.gap_usd_adv"
    },
    "Table C": {
        "indicator_label": "The Total Value Gaps Identified Between 134 Developing Countries and 36 Advanced Economies, 2009–2018, as a Percent of Total Trade",
        "indicator_code": "GFI.TableC.gap_pct_adv"
    },
    "Table E": {
        "indicator_label": "The Sums of the Value Gaps Identified in Trade Between 134 Developing Countries and all of their Global Trading Partners, 2009–2018 in USD Millions",
        "indicator_code": "GFI.TableE.gap_usd_all"
    },
    "Table G": {
        "indicator_label": "The Total Value Gaps Identified in Trade Between 134 Developing Countries and all of their Trading Partners, 2009–2018 as a Percent of Total Trade",
        "indicator_code": "GFI.TableG.gap_pct_all"
    }
}