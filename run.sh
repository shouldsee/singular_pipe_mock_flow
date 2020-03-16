BIN=${1:-python3}
rm -rf /tmp/test_remote/ -rf
$BIN -m spiper run \
  spiper_mock_flow@file://$PWD \
  TOPLEVEL:run_and_backup \
  /tmp/test_remote/root 1 2 /tmp/test_remote/root.backup

