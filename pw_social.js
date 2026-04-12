const { chromium } = require('@playwright/test');
(async () => {
  const b = await chromium.launch();
  const p = await b.newPage();
  await p.goto('http://localhost:5001', {waitUntil:'networkidle'});
  await p.getByText('Social').click();
  await p.waitForTimeout(1500);
  await p.screenshot({path:'/tmp/social_stories.png'});
  await b.close();
})();
