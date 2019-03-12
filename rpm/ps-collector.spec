Name:      ps-collector
Version:   0.1.0
Release:   1%{?dist}
Summary:   perfSonar data collector
Packager:  SAND-CI
Group:     Applications/Monitoring
License:   Apache 2.0
URL:       https://sand-ci.org

Source0:   %{name}-%{version}.tar.gz

Requires(pre): shadow-utils

BuildRequires:  python2-devel
BuildRequires:  python-setuptools

BuildRequires: systemd
%{?systemd_requires}

BuildArch: noarch

# Requires: python2-schedule

%description
%{summary}

%prep
%setup -n %{name}-%{version}

%build
%py2_build

%install
%py2_install

%pre
getent group pscollector >/dev/null || groupadd -f -r pscollector
if ! getent passwd pscollector >/dev/null ; then
   useradd -r -g pscollector -d %{_localstatedir}/lib/%{name} -s /sbin/nologin -c "Runtime account for the ps-collector" pscollector
fi
exit 0

%files
%doc README
%defattr(-,root,root,-)
%config %{_sysconfdir}/%{name}/config.ini
%config %{_sysconfdir}/%{name}/logging-config.ini
%config(noreplace) %{_sysconfdir}/%{name}/config.d/*
%attr(-,pscollector,pscollector)  %{_localstatedir}/lib/%{name}
%{_bindir}/%{name}
%{_unitdir}/%{name}.service

%{python2_sitelib}/ps_collector
%{python2_sitelib}/ps_collector-*.egg-info

%post
%systemd_post ps-collector.service

%preun
%systemd_preun ps-collector.service

%postun
%systemd_postun_with_restart ps-collector.service

%changelog
* Tue Dec 18 2018 Brian Bockelman <bbockelm@cse.unl.edu> - 0.1.0-1
- Initial version of the new ps-collector software.

