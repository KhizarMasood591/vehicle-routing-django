function progress() {
    const modelBtn = document.querySelector("form.model-params button");
    modelBtn.style.display = "none";
    const progressCont = document.querySelector(".progress");
    progressCont.style.display = "flex";
    const bar = document.querySelector(".progress-bar");
    const progressPct = document.querySelector(".progress-bar p");
    for (let i=0; i<=60; i++){
        const pct = (i/60)*100;
        setTimeout(
            function(){
                progressPct.innerHTML = pct.toFixed(0).toString()+"%";
                bar.style.width=pct.toFixed(0).toString()+"%";
            },
            1000
        );
    };
}