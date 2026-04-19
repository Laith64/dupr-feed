const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true });
  await page.goto('http://localhost:5001');
  await page.waitForTimeout(3500);

  await page.evaluate(() => openPlayerProfile('5374679100', 'Itziar Rios'));
  await page.waitForTimeout(5500);

  // Scroll to graph
  await page.evaluate(() => {
    const sec = document.getElementById('profile-graph-section');
    if (sec) sec.scrollIntoView({ block: 'start' });
  });
  await page.waitForTimeout(800);
  await page.screenshot({ path: '/tmp/graph-default.png' });

  // Check delta presence + that bottom nav is visible
  const info = await page.evaluate(() => {
    const dEl = document.getElementById('profile-graph-delta');
    const navEl = document.querySelector('.topnav-tabs');
    const navR = navEl.getBoundingClientRect();
    return {
      deltaText: dEl ? dEl.textContent.trim() : null,
      deltaCls: dEl ? dEl.className : null,
      navVisible: navR.height > 0 && navR.bottom <= window.innerHeight + 1,
      navBottom: navR.bottom,
      vp: window.innerHeight,
    };
  });
  console.log('default (All-time):', JSON.stringify(info));

  // Switch to Year
  await page.evaluate(() => document.querySelector('.pgraph-time-btn[data-graphtime="year"]').click());
  await page.waitForTimeout(600);
  await page.screenshot({ path: '/tmp/graph-year.png' });
  const y = await page.evaluate(() => document.getElementById('profile-graph-delta').textContent.trim());
  console.log('Year delta:', y);

  // Month
  await page.evaluate(() => document.querySelector('.pgraph-time-btn[data-graphtime="month"]').click());
  await page.waitForTimeout(600);
  await page.screenshot({ path: '/tmp/graph-month.png' });
  const mo = await page.evaluate(() => document.getElementById('profile-graph-delta').textContent.trim());
  console.log('Month delta:', mo);

  // Switch to singles
  await page.evaluate(() => document.getElementById('pgraph-singles-btn').click());
  await page.waitForTimeout(700);
  await page.screenshot({ path: '/tmp/graph-singles-month.png' });
  const sm = await page.evaluate(() => document.getElementById('profile-graph-delta').textContent.trim());
  console.log('Singles month delta:', sm);

  // Verify the old "DUPR Change" card is GONE
  const oldCard = await page.evaluate(() => document.querySelector('.dupr-delta-card'));
  console.log('old card present?', !!oldCard);

  await browser.close();
})();
