@prg
Feature: PRG delta PSR projection per clock window

  PRG projects one payment status report (PSR) per client per clock window.
  Only rows whose status moved past the client's watermark are emitted (R-29);
  the deepest response leg wins per row (R-17: PBSR > SBSR > ISR > CTV_PASS).
  A window with no movement emits a zero-valued heartbeat PSR (SCRUM-55) so
  the consumer can tell "no movement" from "PRG dead"; a resend re-projects
  everything.

  Background:
    Given a PRG client with a new arrival

  Scenario: First window projects every known-status row with the deepest leg winning
    Given transaction "T1" has SBSR status "ACSP" and PBSR status "RJCT"
    And transaction "T2" has only ISR status "ACSP"
    And transaction "T3" has only a CTV PASS verdict
    When the PRG window "w1" runs
    Then the PRG job completes
    And the PSR file for window "w1" reports exactly:
      | transaction | status   |
      | T1          | RJCT     |
      | T2          | ACSP     |
      | T3          | CTV_PASS |

  Scenario: An unchanged window emits a zero-valued heartbeat PSR
    Given transaction "T1" has SBSR status "ACSP" and PBSR status "ACSC"
    And transaction "T2" has only a CTV PASS verdict
    And the PRG window "w1" has already emitted
    When the PRG window "w2" runs
    Then the PRG job completes
    And the PSR file for window "w2" is a zero-valued heartbeat
    And the client watermark is unchanged by the heartbeat

  Scenario: A single status flip projects exactly that row in the next window
    Given transaction "T1" has SBSR status "ACSP" and PBSR status "ACSC"
    And transaction "T2" has only a CTV PASS verdict
    And the PRG window "w1" has already emitted
    When the PBSR status of transaction "T1" flips to "RJCT"
    And the PRG window "w2" runs
    Then the PRG job completes
    And the PSR file for window "w2" reports exactly:
      | transaction | status |
      | T1          | RJCT   |

  Scenario: A resend re-projects all current rows ignoring the watermark
    Given transaction "T1" has SBSR status "ACSP" and PBSR status "ACSC"
    And transaction "T2" has only ISR status "ACSP"
    And transaction "T3" has only a CTV PASS verdict
    And the PRG window "w1" has already emitted
    When the PRG window "w2" runs as a resend
    Then the PRG job completes
    And the PSR file for window "w2" reports exactly:
      | transaction | status   |
      | T1          | ACSC     |
      | T2          | ACSP     |
      | T3          | CTV_PASS |

  Scenario: A mid-DAG row with unknown status is excluded and made visible with a WARN
    Given transaction "T1" has only a CTV PASS verdict
    And transaction "T2" has no verdict yet
    When the PRG window "w1" runs
    Then the PRG job completes
    And the PSR file for window "w1" reports exactly:
      | transaction | status   |
      | T1          | CTV_PASS |
    And exactly one WARN reports transaction "T2" excluded for stage "PRG" with reason "STATUS_UNKNOWN"

  Scenario: Watermarks advance only for emitted rows
    Given transaction "T1" has only ISR status "ACSP"
    And transaction "T2" has no verdict yet
    When the PRG window "w1" runs
    Then the PRG job completes
    And the client watermark holds exactly:
      | transaction | status |
      | T1          | ACSP   |
