#
# spec file for package benji
# created for openSUSE
#
# Copyright (c) Michael Vetter <jubalh@iodoru.org>
#
#


Name:           benji
Version:        0.0.0
Release:        0
Summary:        Deduplicating block based backup software
License:        LGPL-3.0-only
Group:          Productivity/Archiving/Backup
URL:            https://benji-backup.me/
Source0:        https://github.com/elemental-lf/benji/archive/v0.0.0.tar.gz
BuildRequires:  python3-devel >= 3.6.5
BuildRequires:  python3-setuptools
Requires:       python3-PrettyTable
Requires:       python3-alembic
Requires:       python3-dateutil
Requires:       python3-psutil
Requires:       python3-setproctitle
Requires:       python3-shortuuid
Requires:       python3-sqlalchemy
Recommends:     python3-boto3
Recommends:     python3-psycopg2
BuildArch:      noarch

%description
Deduplicating block based backup software for ceph/rbd,
image files and devices.

%prep
%setup -q

%build
python3 setup.py build

%install
mkdir -p %{buildroot}%{_localstatedir}/lib/benji
python3 setup.py install --single-version-externally-managed --root=%{buildroot}
mkdir -p %{buildroot}%{_sysconfdir}/
cp etc/benji.yaml %{buildroot}%{_sysconfdir}/benji.yaml

%files
%doc README.rst
%license LICENSE.txt
%{_bindir}/benji
%{_libexecdir}/python3.6/site-packages/
%{_sysconfdir}/benji.yaml

%changelog

