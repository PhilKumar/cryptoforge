(function () {
    function earlyFmtDt(raw) {
      if (!raw || raw === 'None') return '—';
      var s = String(raw).trim().replace('T', ' ');
      var full = s.match(/(\d{4})-(\d{2})-(\d{2}).*?(\d{2}:\d{2}:\d{2})/);
      if (full) {
        var months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        var monthIdx = Math.max(0, Math.min(11, parseInt(full[2], 10) - 1));
        return full[3] + ' ' + months[monthIdx] + ' ' + full[1] + ', ' + full[4] + ' IST';
      }
      var timeOnly = s.match(/(\d{2}:\d{2}:\d{2})/);
      if (timeOnly) return timeOnly[1] + ' IST';
      return s;
    }
    window.fmtDt = earlyFmtDt;
  })();
