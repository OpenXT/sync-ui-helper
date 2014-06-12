%include common.inc

Name: sync-ui-helper
Summary: XenClient Synchronizer XT UI helper
Source0: %{name}.tar.gz
BuildArch: noarch
Requires: sync-database = %{version}

%define desc Database access tool used by the XenClient XT Synchronizer UI.

%include description.inc
%include python.inc
