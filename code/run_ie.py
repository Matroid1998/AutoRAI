"""
CLI entry point for running the IE pipeline.

Usage:
    python run_ie.py --section I --hadm_id 12345678 --output output/ie/
    python run_ie.py --section N --hadm_id 12345678
    python run_ie.py --section all --hadm_id 12345678
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

# Add code/ to path so imports work
sys.path.insert(0, str(Path(__file__).parent))

from ie.config import IEConfig, LLMConfig, MIMICPaths, get_config, set_config
from ie.core.llm_client import LLMClient
from ie.core.orchestrator import IEOrchestrator


def setup_logging(level: str = "INFO") -> None:
    """Configure logging for the IE pipeline."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(name)-35s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def get_adapter(section: str, config: IEConfig):
    """Get the appropriate adapter for the given section."""
    if section.upper() == "I":
        from ie.adapters.mds_mimic.section_i_adapter import SectionIAdapter
        return SectionIAdapter(config)
    elif section.upper() == "N":
        from ie.adapters.mds_mimic.section_n_adapter import SectionNAdapter
        return SectionNAdapter(config)
    elif section.upper() == "O":
        from ie.adapters.mds_mimic.section_o_adapter import SectionOAdapter
        return SectionOAdapter(config)
    else:
        raise ValueError(f"Unknown section: {section}. Use I, N, or O.")


def run_section(
    section: str,
    hadm_id: str,
    config: IEConfig,
    llm_client: LLMClient,
    output_dir: Path,
) -> dict:
    """Run the IE pipeline for one section."""
    logger = logging.getLogger(__name__)
    logger.info(f"\n{'='*60}")
    logger.info(f"Running IE for Section {section.upper()}, hadm_id={hadm_id}")
    logger.info(f"{'='*60}")

    adapter = get_adapter(section, config)
    orchestrator = IEOrchestrator(adapter=adapter, llm_client=llm_client)
    evidence_package = orchestrator.run(episode_id=hadm_id)

    # Save output
    output_path = output_dir / f"evidence_section_{section.upper()}_{hadm_id}.json"
    evidence_package.to_json(output_path)
    logger.info(f"Evidence package saved to: {output_path}")

    # Print summary
    summary = evidence_package.summary()
    logger.info(f"Summary: {json.dumps(summary, indent=2)}")

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Run the IE pipeline for MDS form completion on MIMIC-IV data."
    )
    parser.add_argument(
        "--section",
        type=str,
        required=True,
        help="MDS section to process: I, N, O, or 'all' for all sections.",
    )
    parser.add_argument(
        "--hadm_id",
        type=str,
        required=True,
        help="Hospital admission ID (hadm_id) from MIMIC-IV.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output directory for evidence packages. Default: output/ie/",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=None,
        help="LLM model to use (e.g., 'google/gemini-2.5-flash-preview').",
    )
    parser.add_argument(
        "--api_key",
        type=str,
        default=None,
        help="LLM API key. Can also be set via AUTORAI_LLM_API_KEY env var.",
    )
    parser.add_argument(
        "--base_url",
        type=str,
        default=None,
        help="LLM API base URL. Default: https://openrouter.ai/api/v1",
    )
    parser.add_argument(
        "--log_level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level.",
    )

    args = parser.parse_args()
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)

    # Build configuration
    config = get_config()
    if args.model:
        config.llm.model = args.model
    if args.api_key:
        config.llm.api_key = args.api_key
    if args.base_url:
        config.llm.base_url = args.base_url
    if args.output:
        config.output_dir = Path(args.output)
    set_config(config)

    # Validate API key
    if not config.llm.api_key:
        logger.error(
            "No LLM API key provided. Set AUTORAI_LLM_API_KEY environment "
            "variable or use --api_key flag."
        )
        sys.exit(1)

    # Create output directory
    config.output_dir.mkdir(parents=True, exist_ok=True)

    # Create LLM client
    llm_client = LLMClient(
        model=config.llm.model,
        api_key=config.llm.api_key,
        base_url=config.llm.base_url,
        temperature=config.llm.temperature,
        max_tokens=config.llm.max_tokens,
    )

    # Run sections
    sections = ["I", "N", "O"] if args.section.upper() == "ALL" else [args.section.upper()]
    all_summaries = {}

    for section in sections:
        try:
            summary = run_section(
                section=section,
                hadm_id=args.hadm_id,
                config=config,
                llm_client=llm_client,
                output_dir=config.output_dir,
            )
            all_summaries[f"Section {section}"] = summary
        except Exception as e:
            logger.error(f"Failed to process Section {section}: {e}", exc_info=True)
            all_summaries[f"Section {section}"] = {"error": str(e)}

    # Print final summary
    logger.info(f"\n{'='*60}")
    logger.info("FINAL SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(json.dumps(all_summaries, indent=2))


if __name__ == "__main__":
    main()
