"""
Report formatting helpers for verification scripts.
"""

from .assertions import get_passed, get_failed, GREEN, RED, CYAN, BOLD, RESET


def print_header(title):
    print(f"{BOLD}{CYAN}{'='*60}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{'='*60}{RESET}")


def print_section(title):
    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}  {title}{RESET}")
    print(f"{'='*60}")


def print_summary():
    p = get_passed()
    f = get_failed()
    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}  SUMMARY{RESET}")
    print(f"{'='*60}")
    print(f"  {GREEN}\u2713 Passed: {p}{RESET}")
    if f:
        print(f"  {RED}\u2717 Failed: {f}{RESET}")
    if f == 0:
        print(f"\n  {GREEN}{BOLD}All data assertions passed!{RESET}")
    else:
        print(f"\n  {RED}{BOLD}Some assertions failed — see details above.{RESET}")


def exit_with_status():
    import sys
    sys.exit(1 if get_failed() > 0 else 0)
